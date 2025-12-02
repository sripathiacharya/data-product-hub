from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import os
import json

import pandas as pd
import yaml
from pydantic import BaseModel, Field

import logging
logger = logging.getLogger(__name__)

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
    security: Dict[str, Any] = Field(default_factory=dict)
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
# Runtime model (not Pydantic)
# ------------------------------------------------------------

@dataclass
class DataProductRuntime:
    config: DataProductConfig
    df: pd.DataFrame
    raw: Dict[str, pd.DataFrame] = field(default_factory=dict)


# ------------------------------------------------------------
# Internal registry for loaded products
# ------------------------------------------------------------

# key = route
_REGISTRY: Dict[str, DataProductRuntime] = {}


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
    _REGISTRY.clear()

    for path in config_dir.glob("*.yaml"):
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        cfg = DataProductConfig(**raw)
        repo_root = config_dir.parent.parent
        runtime = build_runtime(cfg, repo_root)
        _REGISTRY[cfg.route] = runtime
        print(f"[config] Loaded YAML config {cfg.id} (route={cfg.route})")


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
    _REGISTRY.clear()

    repo_root_resolved = Path(repo_root) if repo_root else Path(".")

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

        # Normalise route just in case (no leading '/')
        route = cfg.route_key
        _REGISTRY[route] = runtime

    logger.info("Loaded %d data products into registry.", len(_REGISTRY))


# ------------------------------------------------------------
# Core: build runtime from config
# ------------------------------------------------------------

def build_runtime(cfg: DataProductConfig, repo_root: Path) -> DataProductRuntime:
    backend = cfg.backend

    if backend.engine != "parquet_join":
        raise ValueError(f"Unsupported backend engine: {backend.engine}")

    # --------------------------------------------------------
    # 1. Load all source parquet files
    # --------------------------------------------------------
    frames: Dict[str, pd.DataFrame] = {}

    for name, src in backend.sources.items():
        full_path = repo_root / src.path
        if not full_path.exists():
            raise FileNotFoundError(f"Parquet not found for source '{name}': {full_path}")

        df = pd.read_parquet(full_path)

        # apply rename
        if src.rename:
            df = df.rename(columns=src.rename)

        frames[name] = df

    # --------------------------------------------------------
    # 2. Apply joins in sequence
    # --------------------------------------------------------
    if backend.joins:
        first_join = backend.joins[0]
        base_name = first_join.left
        if base_name not in frames:
            raise ValueError(f"Unknown join base frame '{base_name}'")

        base_df = frames[base_name]

        for join in backend.joins:
            if not join.on:
                raise ValueError(
                    f"Join between '{join.left}' and '{join.right}' has no 'on:' columns configured"
                )

            if join.left == base_name:
                left_df = base_df
            else:
                left_df = frames[join.left]

            right_df = frames[join.right]

            base_df = left_df.merge(
                right_df,
                on=join.on,
                how="inner",
            )

        df = base_df

    else:
        # no joins: must be exactly one source
        if len(frames) != 1:
            raise ValueError("Multiple sources provided but no joins defined.")
        df = next(iter(frames.values()))

    # --------------------------------------------------------
    # 3. Enforce expected columns & generate missing key column
    # --------------------------------------------------------

    for col in cfg.entity.columns:
        if not col.generated and col.name not in df.columns:
            raise ValueError(
                f"Required column '{col.name}' missing after joins in product '{cfg.id}'"
            )

    key_col = cfg.entity.key_column
    if key_col not in df.columns:
        df = df.reset_index(drop=True)
        df[key_col] = df.index.astype(str)

    # --------------------------------------------------------
    # 4. Apply type conversions
    # --------------------------------------------------------
    for col in cfg.entity.columns:
        name = col.name
        if col.generated:
            continue
        if name not in df.columns:
            continue

        if col.type == "datetime":
            df[name] = pd.to_datetime(df[name])
        elif col.type == "int":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif col.type == "float":
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif col.type == "string":
            df[name] = df[name].astype(str)

    return DataProductRuntime(config=cfg, df=df, raw=frames)


# ------------------------------------------------------------
# Public API for router
# ------------------------------------------------------------

def get_runtime(route: str) -> DataProductRuntime:
    key = route.lstrip("/")
    if key not in _REGISTRY:
        raise KeyError(f"Data product route '{route}' not found")
    return _REGISTRY[key]



def list_products() -> List[DataProductConfig]:
    return [rt.config for rt in _REGISTRY.values()]
