# src/operator/main.py
import logging

import kopf  # noqa: F401  # needed so 'kopf run' works
from handlers import dataproduct_handler  # noqa: F401

logging.basicConfig(level=logging.INFO)

# No extra code needed here.
# Run with: `kopf run src/operator/main.py`
