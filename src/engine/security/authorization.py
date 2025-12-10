# src/engine/security/authorization.py
from typing import Optional, Dict, Any
import logging

from fastapi import HTTPException, status

from .config import AUTH_CONFIG

logger = logging.getLogger(__name__)


def check_dataset_access(runtime, principal: Optional[Dict[str, Any]]) -> None:
    """
    Enforce per-dataset authPolicy:

      - If auth disabled globally -> allow.
      - If security.authPolicy == "none" -> allow.
      - If "optional":
          - with or without token -> allow.
      - If "required":
          - token (principal) must be present and valid (already validated).
    """
    if not getattr(AUTH_CONFIG, "enabled", False):
        return

    sec = getattr(runtime.config, "security", None)
    policy = sec.authPolicy if sec is not None else "none"

    if policy == "none":
        return

    if policy == "optional":
        # Token may be missing; we don't care yet.
        return

    if policy == "required":
        if principal is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Access token is required for this data product.",
            )
        # For now: any valid token is enough.
        # Later we plug in Vault-based entitlements here.
        return

    # Just in case someone sets an unknown value:
    logger.warning("Unknown authPolicy=%r on %s; denying by default.", policy, runtime.config.id)
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access to this data product is forbidden.",
    )
