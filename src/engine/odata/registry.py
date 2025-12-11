# src/engine/odata/registry.py

from pathlib import Path
from typing import Any, Dict, List, Optional, Literal
from dataclasses import dataclass, field
import os
import json
import threading

import duckdb
import yaml
from pydantic import BaseModel, Field

import logging

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# DuckDB: single in-process connection & lock
# ------------------------------------------------------------

_DUCKDB_CONN = duckdb.connect(database=":memory:")
_DUCKDB_LOCK = threading.Lock()


def _quote_ident(name: str) -> str:
    # Minimal identifier quoting for DuckDB
    return '"' + str(name).replace('"', '""') + '"'


# ------------------------------------------------------------
# Pydantic configuration models
# ------------------------------------------------------------

class SourceConfig(BaseModel):
    path: str
    rename: Dict[str, str] = Field(default_factory=dict)


class JoinConfig(BaseModel):
    left: str
    right: str
    # 'on' should be optional but default to empty list
    on: List[str] = Field(default_factory=list)


class APISpec(BaseModel):
    path: str
    protocol: str = "odata"
    resource: Optional[str] = None
    version: str = "v1"


class BackendConfig(BaseModel):
    engine: str                           # e.g., "parquet_join"
    sources: Dict[str, SourceConfig]
    joins: List[JoinConfig] = Field(default_factory=list)


class EntityColumn(BaseModel):
    name: str
    type: str                             # "string", "int", "datetime", etc.
    generated: bool = False               # whether the engine should generate this column


class EntityConfig(BaseModel):
    name: str
    key_column: str
    columns: List[EntityColumn]


class ODataConfig(BaseModel):
    max_top: int = 1000
    default_top: int = 100
    filterable: List[str] = Field(default_factory=list)
    orderable: List[str] = Field(default_factory=list)


class SecurityConfig(BaseModel):
    authPolicy: Literal["none", "optional", "required"] = "none"


class DataProductConfig(BaseModel):
    id: str
    route: Optional[str] = None
    api: Optional[APISpec] = None
    description: Optional[str] = None
    namespace: Optional[str] = None
    display_name: Optional[str] = None
    owner: Optional[str] = None
    backend: BackendConfig
    entity: EntityConfig
    odata: ODataConfig = Field(default_factory=ODataConfig)

    # Extra metadata that engine may ignore for now but we keep in model
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    qos: Dict[str, Any] = Field(default_factory=dict)
    deployment_mode: Optional[str] = None

    @property
    def route_key(self) -> str:
        """
        Canonical route key used by the engine/registry.

        Priority:
        1) explicit `route` if present (legacy engine YAML)
        2) `api.path` (from CR / operator metadata)
        3) fallback to `id`
        Always normalised to strip leading '/'.
        """
        if self.route:
            r = self.route
        elif self.api and self.api.path:
            r = self.api.path
        else:
            r = self.id
        return r.lstrip("/")


# ------------------------------------------------------------
# Runtime model (no in-memory DataFrame)
# ------------------------------------------------------------

@dataclass
class DataProductRuntime:
    config: DataProductConfig
    joined_view: str                      # e.g., "dp_southafrica_scheduled_outage_joined"
    source_views: Dict[str, str] = field(default_factory=dict)


# ------------------------------------------------------------
# Internal registry for loaded products
# ------------------------------------------------------------

# key = route_key
_REGISTRY: Dict[str, DataProductRuntime] = {}
_REGISTRY_LOCK = threading.Lock()


# ------------------------------------------------------------
# Helper to determine repo_root
# ------------------------------------------------------------

def _resolve_repo_root(explicit: Optional[Path] = None) -> Path:
    if explicit is not None:
        return explicit

    env_root = os.getenv("DP_REPO_ROOT")
    if env_root:
        return Path(env_root)

    # Fallback: repo root inferred from this file location
    return Path(__file__).resolve().parents[2]


# ------------------------------------------------------------
# Utility: load all YAML configs in a folder (legacy/local)
# ------------------------------------------------------------

def load_config_dir(config_dir: Path) -> None:
    """
    Load all engine YAML configs from a directory into the registry.

    This is mainly used for local/legacy development. In the new model, the
    DataProduct CR is the source of truth, but this function is kept as a
    convenient fallback.
    """
    global _REGISTRY
    with _REGISTRY_LOCK:
        _REGISTRY.clear()

    for path in config_dir.glob("*.yaml"):
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        cfg = DataProductConfig(**raw)
        repo_root = config_dir.parent.parent
        runtime = build_runtime(cfg, repo_root)
        route = cfg.route_key
        with _REGISTRY_LOCK:
            _REGISTRY[route] = runtime
        print(f"[config] Loaded YAML config {cfg.id} (route={route})")


# ------------------------------------------------------------
# Utility: load from dataproducts.json (ConfigMap file)
# ------------------------------------------------------------

def load_from_metadata_file(metadata_path: Path, repo_root: Optional[Path] = None) -> None:
    """
    Load data products from a JSON metadata file (dataproducts.json).
    Used when the ConfigMap is mounted as a file in the pod.
    """
    global _REGISTRY

    if not metadata_path.exists():
        print(f"[metadata] {metadata_path} not found, registry will be empty.")
        with _REGISTRY_LOCK:
            _REGISTRY.clear()
        return

    raw_text = metadata_path.read_text(encoding="utf-8")
    items = json.loads(raw_text)

    if not isinstance(items, list):
        raise ValueError("dataproducts.json must contain a JSON array of data products")

    _load_from_items(items, repo_root=repo_root)


# ------------------------------------------------------------
# Utility: load directly from a DataProduct CR manifest (local mode)
# ------------------------------------------------------------

def load_from_cr_manifest(cr_path: Path, repo_root: Optional[Path] = None) -> None:
    global _REGISTRY

    if not cr_path.exists():
        raise FileNotFoundError(f"DataProduct CR manifest not found: {cr_path}")

    raw = yaml.safe_load(cr_path.read_text(encoding="utf-8"))
    spec = raw.get("spec", {})

    dp_id = raw["metadata"]["name"]
    api = spec.get("api", {})
    backend = spec.get("backend", {})
    entity = spec.get("entity", {})
    odata = spec.get("odata", {})

    cfg = DataProductConfig(
        id=dp_id,
        api=APISpec(
            path=api.get("path", f"/{dp_id}"),
            protocol=api.get("protocol", "odata"),
            resource=api.get("resource"),
            version=api.get("version", "v1"),
        ),
        description=spec.get("description"),
        backend=BackendConfig(**backend),
        entity=EntityConfig(**entity),
        odata=ODataConfig(**odata),
    )

    repo_root_resolved = _resolve_repo_root(repo_root)
    runtime = build_runtime(cfg, repo_root_resolved)

    with _REGISTRY_LOCK:
        _REGISTRY.clear()
        _REGISTRY[cfg.route_key] = runtime

    print(f"[local] Loaded DataProduct CR from {cr_path} (id={cfg.id}, route={cfg.route_key})")


# ------------------------------------------------------------
# Internal helper: populate registry from a list of metadata dicts
# ------------------------------------------------------------

def _load_from_items(items: List[dict], repo_root: Optional[Path] = None) -> None:
    """
    Load a list of data product configs into the in-memory registry.

    Any item that fails due to validation errors or missing data files is
    logged and skipped instead of crashing the whole engine.
    """
    global _REGISTRY
    repo_root_resolved = Path(repo_root) if repo_root else Path(".")

    new_registry: Dict[str, DataProductRuntime] = {}

    for raw in items:
        try:
            cfg = DataProductConfig(**raw)
        except Exception as e:
            logger.error("Invalid data product config %r: %s", raw.get("id"), e)
            continue

        try:
            runtime = build_runtime(cfg, repo_root_resolved)
        except FileNotFoundError as e:
            logger.error(
                "Skipping data product %s: data files not found (%s)",
                cfg.id,
                e,
            )
            continue
        except Exception as e:
            logger.exception(
                "Error building runtime for data product %s: %s",
                cfg.id,
                e,
            )
            continue

        route = cfg.route_key
        new_registry[route] = runtime

    with _REGISTRY_LOCK:
        _REGISTRY = new_registry

    logger.info("Loaded %d data products into registry.", len(_REGISTRY))


# ------------------------------------------------------------
# Core: build runtime from config (create DuckDB views)
# ------------------------------------------------------------

def build_runtime(cfg: DataProductConfig, repo_root: Path) -> DataProductRuntime:
    backend = cfg.backend

    if backend.engine != "parquet_join":
        raise ValueError(f"Unsupported backend engine: {backend.engine}")

    dp_id = cfg.id
    base_view_prefix = f"dp_{dp_id.replace('-', '_')}"
    source_views: Dict[str, str] = {}

    with _DUCKDB_LOCK:
        # 1. Create a view per source
        for name, src in backend.sources.items():
            view_name = f"{base_view_prefix}_{name}"
            full_path = (repo_root / src.path).resolve()

            if not full_path.exists():
                raise FileNotFoundError(f"Parquet not found for source '{name}': {full_path}")

            if src.rename:
                select_cols = [
                    f"{_quote_ident(orig)} AS {_quote_ident(new)}"
                    for orig, new in src.rename.items()
                ]
                select_clause = ", ".join(select_cols)
            else:
                select_clause = "*"

            sql = f"""
                CREATE OR REPLACE VIEW {_quote_ident(view_name)} AS
                SELECT {select_clause}
                FROM read_parquet('{full_path}');
            """
            logger.info("Creating source view for %s: %s", name, sql)
            _DUCKDB_CONN.execute(sql)
            source_views[name] = view_name

        # 2. Create joined view
        if backend.joins:
            first_join = backend.joins[0]
            if first_join.left not in source_views:
                raise ValueError(f"Unknown join base source '{first_join.left}'")

            joined_view = f"{base_view_prefix}_joined"

            # For now: single join chain, similar to previous pandas implementation
            # (we use only first join as pattern; more can be added later)
            join = backend.joins[0]
            left_view = source_views[join.left]
            right_view = source_views[join.right]

            if not join.on:
                raise ValueError(
                    f"Join between '{join.left}' and '{join.right}' has no 'on:' columns configured"
                )

            on_clause = " AND ".join(
                f"L.{_quote_ident(col)} = R.{_quote_ident(col)}" for col in join.on
            )

            sql_joined = f"""
                CREATE OR REPLACE VIEW {_quote_ident(joined_view)} AS
                SELECT L.*, R.*
                FROM {_quote_ident(left_view)} AS L
                JOIN {_quote_ident(right_view)} AS R
                  ON {on_clause};
            """
            logger.info("Creating joined view for %s: %s", cfg.id, sql_joined)
            _DUCKDB_CONN.execute(sql_joined)
        else:
            # No joins: expect a single source
            if len(source_views) != 1:
                raise ValueError("Multiple sources provided but no joins defined.")
            # Pick the single source as the joined_view
            joined_view = next(iter(source_views.values()))

    return DataProductRuntime(config=cfg, joined_view=joined_view, source_views=source_views)


# ------------------------------------------------------------
# Public API for router
# ------------------------------------------------------------

def get_runtime(route: str) -> Optional[DataProductRuntime]:
    key = route.lstrip("/")
    with _REGISTRY_LOCK:
        return _REGISTRY.get(key)


def list_products() -> List[DataProductRuntime]:
    with _REGISTRY_LOCK:
        return list(_REGISTRY.values())


def get_duckdb_connection():
    return _DUCKDB_CONN, _DUCKDB_LOCK
