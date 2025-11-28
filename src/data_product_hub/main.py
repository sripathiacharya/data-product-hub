from pathlib import Path
import os

from fastapi import FastAPI
from odata.router import router as odata_router
from odata.registry import load_config_dir

app = FastAPI(title="data-product-hub")

config_dir_env = os.getenv("CONFIG_DIR")
if config_dir_env:
    CONFIG_DIR = Path(config_dir_env)
else:
    CONFIG_DIR = (
        Path(__file__)
        .resolve()
        .parents[2]  # -> repo root (from src/data_product_hub/main.py)
        / "charts"
        / "data-product-hub"
        / "config"
        / "data-products"
    )

load_config_dir(CONFIG_DIR)
app.include_router(odata_router)
