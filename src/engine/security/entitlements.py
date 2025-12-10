from __future__ import annotations

from typing import Dict, Set
import logging
import threading
import time
import json
import yaml  # you already use pyyaml in the project
import os

import requests  # if you don’t have it yet, add to dependencies

from .config import AUTH_CONFIG

logger = logging.getLogger(__name__)


class EntitlementsBackend:
    def is_allowed(self, app_id: str, dataset_id: str) -> bool:
        raise NotImplementedError


class NoopEntitlementsBackend(EntitlementsBackend):
    def is_allowed(self, app_id: str, dataset_id: str) -> bool:
        # Everything allowed
        return True


class StaticFileEntitlementsBackend(EntitlementsBackend):
    """
    For local/dev:

    entitlements.yaml:

      apps:
        "app-1":
          - southafrica-scheduled-outage-dataset
          - some-other-dataset
        "another-app":
          - southafrica-scheduled-outage-dataset
    """

    def __init__(self, path: str, reload_interval_sec: int = 30):
        self._path = path
        self._reload_interval_sec = reload_interval_sec
        self._lock = threading.Lock()
        self._last_loaded = 0.0
        self._mapping: Dict[str, Set[str]] = {}
        self._load()

    def _load(self) -> None:
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning("Entitlements static file not found: %s", self._path)
            raw = {}

        apps = raw.get("apps", {})
        mapping: Dict[str, Set[str]] = {}

        for app_id, datasets in apps.items():
            mapping[str(app_id)] = set(str(d) for d in (datasets or []))

        with self._lock:
            self._mapping = mapping
            self._last_loaded = time.time()

        logger.info("Loaded entitlements from %s: %d apps", self._path, len(mapping))

    def _ensure_loaded(self) -> None:
        with self._lock:
            if time.time() - self._last_loaded < self._reload_interval_sec:
                return
        self._load()

    def is_allowed(self, app_id: str, dataset_id: str) -> bool:
        self._ensure_loaded()
        with self._lock:
            allowed = self._mapping.get(app_id, set())
            return dataset_id in allowed


class HttpEntitlementsBackend(EntitlementsBackend):
    """
    For prod: front Vault with a small HTTP service.

    Expected API (you can design it):

      GET {base_url}/entitlements?app_id=...&dataset_id=...

    Response 200 JSON:
      { "allowed": true/false }
    """

    def __init__(self, base_url: str, timeout: float = 2.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def is_allowed(self, app_id: str, dataset_id: str) -> bool:
        url = f"{self._base_url}/entitlements"
        params = {"app_id": app_id, "dataset_id": dataset_id}
        try:
            resp = requests.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            return bool(data.get("allowed", False))
        except Exception as e:
            logger.error(
                "Error calling entitlements service %s: %r (app_id=%s, dataset_id=%s)",
                url,
                e,
                app_id,
                dataset_id,
            )
            # you can choose: fail closed (False) or fail open (True)
            return False


def build_entitlements_backend() -> EntitlementsBackend:
    mode = AUTH_CONFIG.entitlements_mode

    if mode == "off":
        logger.info("Entitlements mode=off: allowing all apps to all datasets.")
        return NoopEntitlementsBackend()

    if mode == "static":
        if not AUTH_CONFIG.entitlements_static_file:
            logger.warning("ENTITLEMENTS_STATIC_FILE not set; falling back to 'off' behaviour.")
            return NoopEntitlementsBackend()
        return StaticFileEntitlementsBackend(AUTH_CONFIG.entitlements_static_file)

    if mode == "http":
        if not AUTH_CONFIG.entitlements_http_base_url:
            logger.warning("ENTITLEMENTS_HTTP_BASE_URL not set; falling back to 'off' behaviour.")
            return NoopEntitlementsBackend()
        return HttpEntitlementsBackend(
            AUTH_CONFIG.entitlements_http_base_url,
            AUTH_CONFIG.entitlements_http_timeout,
        )

    logger.warning("Unknown ENTITLEMENTS_MODE=%r; falling back to 'off'.", mode)
    return NoopEntitlementsBackend()


ENTITLEMENTS_BACKEND: EntitlementsBackend = build_entitlements_backend()
