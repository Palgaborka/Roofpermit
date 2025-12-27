from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests


@dataclass
class DetectedJurisdiction:
    system: str  # "energov" | "arcgis"
    base_url: str
    display_name: str
    state: str
    extra: Dict[str, Any]


ENERGOV_HINTS = [
    "tylerhost.net/apps/selfservice",
    "/EnerGovProd/selfservice",
    "/energovprod/selfservice",
    "selfservice/#/search",
    "#/search",
]

ARCGIS_HINTS = [
    "arcgis/rest/services",
    "/FeatureServer",
    "/MapServer",
]


def _safe_city_from_host(host: str) -> str:
    host = (host or "").lower()
    host = host.replace("www.", "")
    # e.g. westpalmbeachfl-energovpub.tylerhost.net -> westpalmbeachfl
    token = re.split(r"[-.]", host)[0] if host else "jurisdiction"
    token = re.sub(r"[^a-z0-9]+", "", token)
    return token or "jurisdiction"


def detect_system_from_url(raw_url: str) -> DetectedJurisdiction:
    if not raw_url or not raw_url.strip():
        raise ValueError("URL is empty")

    url = raw_url.strip()
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("URL must start with http:// or https:// and include a hostname")

    low = url.lower()

    # ArcGIS detection
    if any(h.lower() in low for h in ARCGIS_HINTS):
        # Normalize to the service endpoint (strip query params)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        city = _safe_city_from_host(parsed.netloc)
        return DetectedJurisdiction(
            system="arcgis",
            base_url=base_url,
            display_name=city.replace("fl", "").upper(),
            state="FL",
            extra={"source_url": url},
        )

    # EnerGov detection
    if any(h.lower() in low for h in ENERGOV_HINTS) or "energov" in low:
        # Normalize EnerGov base (keep scheme+host; keep full path through /apps/selfservice/XYZProd#/search if present)
        # We want a stable "search URL" users paste, not necessarily the API endpoints.
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        # Many EnerGov URLs rely on hash fragments for routing; keep it if present
        if parsed.fragment:
            base_url = base_url + "#" + parsed.fragment

        city = _safe_city_from_host(parsed.netloc)
        return DetectedJurisdiction(
            system="energov",
            base_url=base_url,
            display_name=city.replace("fl", "").upper(),
            state="FL",
            extra={"source_url": url},
        )

    raise ValueError("Could not detect system type from URL (expected EnerGov or ArcGIS REST URL)")


def validate_url_reachable(url: str, timeout: int = 12) -> Dict[str, Any]:
    """
    Lightweight validation: try GET with short timeout.
    Returns basic info to show in UI/log.
    """
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "RoofSpy/1.0"})
        return {"ok": True, "status_code": r.status_code, "final_url": str(r.url)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---- Simple JSON registry helpers (independent of your existing jurisdictions module) ----

REGISTRY_FILENAME = "jurisdictions.custom.json"


def load_custom_registry(data_dir: Path) -> Dict[str, Any]:
    p = data_dir / REGISTRY_FILENAME
    if not p.exists():
        return {"jurisdictions": []}
    return json.loads(p.read_text(encoding="utf-8"))


def save_custom_registry(data_dir: Path, payload: Dict[str, Any]) -> None:
    p = data_dir / REGISTRY_FILENAME
    p.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def upsert_custom_jurisdiction(
    data_dir: Path,
    *,
    jurisdiction_id: str,
    display_name: str,
    state: str,
    system: str,
    base_url: str,
    active: bool = True,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    extra = extra or {}
    reg = load_custom_registry(data_dir)
    items = reg.get("jurisdictions", [])

    obj = {
        "id": jurisdiction_id,
        "display_name": display_name,
        "state": state,
        "system": system,
        "base_url": base_url,
        "active": bool(active),
        "extra": extra,
    }

    replaced = False
    for i, it in enumerate(items):
        if it.get("id") == jurisdiction_id:
            items[i] = obj
            replaced = True
            break

    if not replaced:
        items.append(obj)

    reg["jurisdictions"] = items
    save_custom_registry(data_dir, reg)
    return obj
