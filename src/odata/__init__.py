"""
odata package

Provides generic OData routing, registry, and filtering
for dynamically configured data products.
"""

from .router import router
from .registry import load_config_dir, get_runtime, list_products

__all__ = ["router", "load_config_dir", "get_runtime", "list_products"]
