# src/engine/security/config.py
from dataclasses import dataclass
from typing import List, Optional
import os


@dataclass
class AuthConfig:
    enabled: bool
    jwks_url: Optional[str]
    issuer: Optional[str]
    audience: Optional[str]
    algorithms: List[str]

    # NEW: where to read the application id from
    app_id_claim: Optional[str]

    # Entitlements backend
    entitlements_mode: str  # "off", "static", "http"
    entitlements_static_file: Optional[str]
    entitlements_http_base_url: Optional[str]
    entitlements_http_timeout: float


def load_auth_config() -> AuthConfig:
    enabled = os.getenv("AUTH_ENABLED", "false").lower() == "true"

    jwks_url = os.getenv("AUTH_JWKS_URL")
    issuer = os.getenv("AUTH_ISSUER")
    audience = os.getenv("AUTH_AUDIENCE")
    algos = os.getenv("AUTH_ALGORITHMS", "RS256")
    algorithms = [a.strip() for a in algos.split(",") if a.strip()]

    app_id_claim = os.getenv("AUTH_APP_ID_CLAIM", "azp")  # sensible default for many IDPs

    entitlements_mode = os.getenv("ENTITLEMENTS_MODE", "off").lower()
    entitlements_static_file = os.getenv("ENTITLEMENTS_STATIC_FILE")
    entitlements_http_base_url = os.getenv("ENTITLEMENTS_HTTP_BASE_URL")
    entitlements_http_timeout = float(os.getenv("ENTITLEMENTS_HTTP_TIMEOUT_SECONDS", "2"))

    return AuthConfig(
        enabled=enabled,
        jwks_url=jwks_url,
        issuer=issuer,
        audience=audience,
        algorithms=algorithms,
        app_id_claim=app_id_claim,
        entitlements_mode=entitlements_mode,
        entitlements_static_file=entitlements_static_file,
        entitlements_http_base_url=entitlements_http_base_url,
        entitlements_http_timeout=entitlements_http_timeout,
    )


AUTH_CONFIG = load_auth_config()
