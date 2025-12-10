# src/engine/security/dependency.py
from typing import Optional, Dict, Any
import logging

from fastapi import Depends, HTTPException, Request, status

from .config import AUTH_CONFIG  # if you built this, otherwise stub it
from .jwt_auth import decode_jwt  # or stub

logger = logging.getLogger(__name__)


async def get_current_principal(request: Request) -> Optional[Dict[str, Any]]:
    """
    Decode JWT if present & global auth is enabled.

    Returns claims dict or None.
    """
    if not getattr(AUTH_CONFIG, "enabled", False):
        return None

    auth = request.headers.get("Authorization") or request.headers.get("authorization")
    if not auth:
        return None

    parts = auth.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header.",
        )

    token = parts[1]

    try:
        claims = decode_jwt(token)
        return claims
    except Exception as e:
        logger.warning("JWT validation failed: %r", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token.",
        )
