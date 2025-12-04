from pathlib import Path
import os
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from .odata.router import router as odata_router
from .odata.registry import (
    load_config_dir,
    load_from_metadata_file,
    load_from_cr_manifest,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application startup/shutdown.

    Config source precedence (startup):

      1. DP_LOCAL_CR      -> load directly from a DataProduct CR YAML
      2. DP_METADATA_PATH -> load from dataproducts.json (ConfigMap)
      3. CONFIG_DIR       -> load engine YAML configs from a directory
      4. Otherwise: start with empty registry
    """
    repo_root = Path(os.getenv("DP_REPO_ROOT", "/app"))

    dp_local_cr = os.getenv("DP_LOCAL_CR")
    dp_metadata_path = os.getenv("DP_METADATA_PATH")
    config_dir_env = os.getenv("CONFIG_DIR")

    try:
        if dp_local_cr:
            cr_path = Path(dp_local_cr)
            logger.info("DP_LOCAL_CR set, loading from CR manifest: %s", cr_path)
            load_from_cr_manifest(cr_path, repo_root=repo_root)

        elif dp_metadata_path:
            metadata_path = Path(dp_metadata_path)
            logger.info("DP_METADATA_PATH set, loading from metadata file: %s", metadata_path)
            load_from_metadata_file(metadata_path, repo_root=repo_root)

        elif config_dir_env:
            config_dir = Path(config_dir_env)
            logger.info("CONFIG_DIR set, loading YAML configs from: %s", config_dir)
            # If load_config_dir ever needs repo_root, you can extend its signature later.
            load_config_dir(config_dir)

        else:
            logger.info("No config source set (DP_LOCAL_CR/DP_METADATA_PATH/CONFIG_DIR), "
                        "starting with empty registry.")
    except Exception as e:
        logger.exception("Failed to load data products at startup: %s", e)

    yield
    # no explicit shutdown logic yet


app = FastAPI(title="data-product-hub", lifespan=lifespan)

# OData routes
app.include_router(odata_router)


@app.post("/internal/reload-config")
def reload_config():
    """
    Reload configuration at runtime, using the same precedence as startup:

      1. DP_LOCAL_CR      -> re-load from the local DataProduct CR YAML.
      2. DP_METADATA_PATH -> re-load from the dataproducts.json file.
      3. CONFIG_DIR       -> re-load YAML configs from that directory.

    Otherwise: no-op.
    """
    repo_root = Path(os.getenv("DP_REPO_ROOT", "/app"))

    dp_local_cr = os.getenv("DP_LOCAL_CR")
    dp_metadata_path = os.getenv("DP_METADATA_PATH")
    config_dir_env = os.getenv("CONFIG_DIR")

    if dp_local_cr:
        cr_path = Path(dp_local_cr)
        logger.info("Reloading configuration from local CR: %s", cr_path)
        load_from_cr_manifest(cr_path, repo_root=repo_root)
        return {"status": "ok", "mode": "local-cr", "path": str(cr_path)}

    if dp_metadata_path:
        metadata_path = Path(dp_metadata_path)
        logger.info("Reloading configuration from metadata file: %s", metadata_path)
        load_from_metadata_file(metadata_path, repo_root=repo_root)
        return {"status": "ok", "mode": "metadata-file", "path": str(metadata_path)}

    if config_dir_env:
        config_dir = Path(config_dir_env)
        logger.info("Reloading configuration from CONFIG_DIR: %s", config_dir)
        load_config_dir(config_dir)
        return {"status": "ok", "mode": "config-dir", "path": str(config_dir)}

    logger.info("reload-config: no configuration source set, no-op.")
    return {"status": "no-op", "reason": "no configuration source set"}
