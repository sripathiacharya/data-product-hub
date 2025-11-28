from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

import pandas as pd
import yaml
from pydantic import BaseModel, Field


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
    route: str
    description: Optional[str] = None
    backend: BackendConfig
    entity: EntityConfig
    odata: ODataConfig = Field(default_factory=ODataConfig)


# ------------------------------------------------------------
# Runtime model (not Pydantic)
# ------------------------------------------------------------

@dataclass
class DataProductRuntime:
    config: DataProductConfig
    df: pd.DataFrame


# ------------------------------------------------------------
# Internal registry for loaded products
# ------------------------------------------------------------

_REGISTRY: Dict[str, DataProductRuntime] = {}   # key = route


# ------------------------------------------------------------
# Utility: load all YAML configs in a folder
# ------------------------------------------------------------

def load_config_dir(config_dir: Path) -> None:
    for path in config_dir.glob("*.yaml"):
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        cfg = DataProductConfig(**raw)

        # DEBUG: print join configs
        print(f"Loaded config {cfg.id}")
        for j in cfg.backend.joins:
            print("  join:", j.left, "->", j.right, "on:", j.on)

        repo_root = config_dir.parent.parent
        runtime = build_runtime(cfg, repo_root)
        _REGISTRY[cfg.route] = runtime



# ------------------------------------------------------------
# Build a Parquet-joined DataFrame for a product
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

    return DataProductRuntime(config=cfg, df=df)


# ------------------------------------------------------------
# Public API for router
# ------------------------------------------------------------

def get_runtime(route: str) -> DataProductRuntime:
    if route not in _REGISTRY:
        raise KeyError(f"Data product route '{route}' not found")
    return _REGISTRY[route]


def list_products() -> List[DataProductConfig]:
    return [rt.config for rt in _REGISTRY.values()]
