"""
data_product_hub package

This package contains the FastAPI application used to expose
data products via OData-compatible endpoints.
"""

from .main import app

__all__ = ["app"]
