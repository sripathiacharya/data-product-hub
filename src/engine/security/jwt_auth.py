# src/engine/security/jwt_auth.py
from typing import Dict, Any, Optional
import logging

import jwt
from jwt import PyJWKClient

from .config import AUTH_CONFIG

logger = logging.getLogger(__name__)

_jwk_client: Optional[PyJWKClient] = None


def _get_jwk_client() -> PyJWKClient:
    global _jwk_client
    if _jwk_client is None:
        if not AUTH_CONFIG.jwks_url:
            raise RuntimeError("AUTH_JWKS_URL not set but auth is enabled.")
        _jwk_client = PyJWKClient(AUTH_CONFIG.jwks_url)
    return _jwk_client


def decode_jwt(token: str) -> Dict[str, Any]:
    """
    Validate and decode a JWT access token using JWKS.

    Raises jwt.PyJWTError on validation error.
    """
    jwk_client = _get_jwk_client()
    signing_key = jwk_client.get_signing_key_from_jwt(token)

    options = {"verify_aud": AUTH_CONFIG.audience is not None}

    decoded = jwt.decode(
        token,
        signing_key.key,
        algorithms=AUTH_CONFIG.algorithms,
        audience=AUTH_CONFIG.audience,
        issuer=AUTH_CONFIG.issuer,
        options=options,
    )
    return decoded
