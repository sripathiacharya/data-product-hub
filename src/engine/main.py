from pathlib import Path
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .odata.router import router as odata_router
from .odata.registry import (
    load_config_dir,
    load_from_metadata_file,
    load_from_cr_manifest,
)

import logging
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Configuration sources (in order of precedence)
# -------------------------------------------------------------------
# 1. DP_LOCAL_CR      -> Local development: load directly from DataProduct CR YAML
# 2. DP_METADATA_PATH -> In-cluster: load from dataproducts.json (mounted ConfigMap)
# 3. CONFIG_DIR       -> Legacy/local: load engine YAML configs from a directory
# -------------------------------------------------------------------

DP_LOCAL_CR = os.getenv("DP_LOCAL_CR")
DP_METADATA_PATH = os.getenv("DP_METADATA_PATH")
CONFIG_DIR_ENV = os.getenv("CONFIG_DIR")

CONFIG_DIR = Path(CONFIG_DIR_ENV) if CONFIG_DIR_ENV else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    metadata_path = os.getenv("DP_METADATA_PATH")
    repo_root = os.getenv("DP_REPO_ROOT", "/app")

    if metadata_path:
        logger.info("DP_METADATA_PATH set, loading from metadata file: %s", metadata_path)
        try:
            load_from_metadata_file(Path(metadata_path), repo_root=Path(repo_root))
        except Exception as e:
            logger.exception("Failed to load data products from %s: %s", metadata_path, e)
    else:
        logger.info("DP_METADATA_PATH not set, starting with empty registry.")

    yield



app = FastAPI(title="data-product-hub", lifespan=lifespan)

# OData routes
app.include_router(odata_router)


@app.post("/internal/reload-config")
def reload_config():
    """
    Reload configuration at runtime.

    - If DP_LOCAL_CR is set: re-load from the local DataProduct CR.
      (Handy if you edit the CR file while running locally.)

    - If DP_METADATA_PATH is set: re-load from the dataproducts.json file.

    - If CONFIG_DIR is set: re-load YAML configs from that directory.

    Otherwise: no-op.
    """
    if DP_LOCAL_CR:
        cr_path = Path(DP_LOCAL_CR)
        load_from_cr_manifest(cr_path)
        return {"status": "ok", "mode": "local-cr", "path": str(cr_path)}

    if DP_METADATA_PATH:
        metadata_path = Path(DP_METADATA_PATH)
        load_from_metadata_file(metadata_path)
        return {"status": "ok", "mode": "metadata-file", "path": str(metadata_path)}

    if CONFIG_DIR is not None:
        load_config_dir(CONFIG_DIR)
        return {"status": "ok", "mode": "config-dir", "path": str(CONFIG_DIR)}

    return {"status": "no-op", "reason": "no configuration source set"}
