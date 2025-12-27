# parcels.py
from __future__ import annotations

import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# ----------------------------
# Config
# ----------------------------
OVERPASS_URL = os.environ.get("OVERPASS_URL", "https://overpass-api.de/api/interpreter").strip()
USER_AGENT = os.environ.get("USER_AGENT", "RoofSpy/1.0 (+https://example.com)").strip() or "RoofSpy/1.0"

# Palm Beach County Open Data dataset (as seen on their ArcGIS Hub/Open Data site)
# This is the dataset slug you referenced: "Parcels and Property Details WebMercator"
# We'll attempt multiple URL patterns because ArcGIS Hub deployments vary.
PBC_DATASET_SLUG = "parcels-and-property-details-webmercator"
PBC_OPEN_DATA_HOST = os.environ.get("PBC_OPEN_DATA_HOST", "opendata2-pbcgov.opendata.arcgis.com").strip()

# How many results we allow from Overpass per tile request
OVERPASS_TILE_LIMIT = int(os.environ.get("OVERPASS_TILE_LIMIT", "2000"))
# How large each tile is (in degrees). Smaller = more complete, but more requests.
# 0.01 deg ~ 0.7 miles north-south in FL; tweak if desired.
TILE_DEG = float(os.environ.get("OVERPASS_TILE_DEG", "0.012"))

# ----------------------------
# Helpers
# ----------------------------

def _clean(s: Optional[str]) -> str:
    return (s or "").strip()

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _bbox(latlngs: List[List[float]]) -> Tuple[float, float, float, float]:
    lats = [p[0] for p in latlngs]
    lngs = [p[1] for p in latlngs]
    return min(lats), min(lngs), max(lats), max(lngs)

def _centroid(latlngs: List[List[float]]) -> Tuple[float, float]:
    # Simple polygon centroid approximation (average of vertices)
    # Good enough for county detection.
    lat = sum(p[0] for p in latlngs) / max(1, len(latlngs))
    lng = sum(p[1] for p in latlngs) / max(1, len(latlngs))
    return lat, lng

def _point_in_poly(lat: float, lng: float, poly: List[List[float]]) -> bool:
    # Ray-casting algorithm
    inside = False
    n = len(poly)
    if n < 3:
        return False
    j = n - 1
    for i in range(n):
        yi, xi = poly[i][0], poly[i][1]
        yj, xj = poly[j][0], poly[j][1]
        intersect = ((xi > lng) != (xj > lng)) and (
            lat < (yj - yi) * (lng - xi) / ((xj - xi) if (xj - xi) != 0 else 1e-12) + yi
        )
        if intersect:
            inside = not inside
        j = i
    return inside

def _http_get(url: str, timeout: int = 25) -> requests.Response:
    return requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
        allow_redirects=True,
    )

def _http_post(url: str, data: str, timeout: int = 60) -> requests.Response:
    return requests.post(
        url,
        data=data.encode("utf-8"),
        timeout=timeout,
        headers={"User-Agent": USER_AGENT, "Content-Type": "text/plain"},
    )

# ----------------------------
# 1) County detection via Overpass admin boundary for centroid
# ----------------------------

def _detect_county_name(lat: float, lng: float) -> str:
    """
    Uses Overpass to find the admin boundary that contains the centroid.
    We ask for boundary=administrative admin_level=6 (county level in the US),
    then return its name if found.
    """
    # Around radius small; containing relation should still match
    q = f"""
    [out:json][timeout:25];
    (
      is_in({lat},{lng})->.a;
      rel(pivot.a)["boundary"="administrative"]["admin_level"="6"];
    );
    out tags;
    """
    try:
        r = _http_post(OVERPASS_URL, q, timeout=60)
        r.raise_for_status()
        j = r.json()
        for el in j.get("elements", []):
            tags = el.get("tags") or {}
            name = (tags.get("name") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return ""

def _is_palm_beach_county(latlngs: List[List[float]]) -> bool:
    lat, lng = _centroid(latlngs)
    county = _detect_county_name(lat, lng).lower()
    return "palm beach" in county and "county" in county

# ----------------------------
# 2) Palm Beach County open-data attempt (best-effort)
# ----------------------------

def _try_pbc_open_data(latlngs: List[List[float]], limit: int) -> Optional[List[Dict[str, str]]]:
    """
    Best-effort attempt to pull parcel/property records from PBC Open Data.
    ArcGIS Hub URLs vary; we try a few common patterns.

    If this fails (non-200, missing fields, etc), return None and we fall back to OSM.
    """
    # polygon as ESRI rings (WGS84)
    ring = [[p[1], p[0]] for p in latlngs]  # [lng, lat]
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    # Common ArcGIS Hub/OD patterns:
    # 1) "api/v3/datasets/<slug>/query" is not standard, but some hubs expose query endpoints.
    # 2) The most reliable is often a FeatureServer query URL (but hubs hide it behind JS).
    #
    # Because we can't guarantee the internal FeatureServer URL, we do this:
    # - Try to fetch the dataset landing page JSON-ish endpoints that some hubs expose.
    # - If not possible, we fail gracefully.

    # Some hubs expose a v3 dataset metadata endpoint:
    # https://opendata.arcgis.com/api/v3/datasets/<datasetId>
    # But we only have a slug; try hub's internal "api/v3/datasets" search:
    # https://opendata.arcgis.com/api/v3/datasets?filter[slug]=<slug>
    base_api = "https://opendata.arcgis.com/api/v3/datasets"
    try:
        meta_url = f"{base_api}?filter[slug]={PBC_DATASET_SLUG}"
        mr = _http_get(meta_url, timeout=25)
        if mr.status_code != 200:
            return None
        meta = mr.json()
        data = (meta.get("data") or [])
        if not data:
            return None

        dataset_id = data[0].get("id")
        # "relationships" sometimes include "layers" with "url"
        rel = data[0].get("relationships") or {}
        layers = (rel.get("layers") or {}).get("data") or []

        feature_layer_url = None

        # If layer objects are not expanded, try the "included" section
        included = meta.get("included") or []
        by_id = {(x.get("type"), x.get("id")): x for x in included if x.get("id") and x.get("type")}

        for layer_ref in layers:
            lid = layer_ref.get("id")
            obj = by_id.get(("layer", lid)) or {}
            attrs = obj.get("attributes") or {}
            u = attrs.get("url")
            if u and ("FeatureServer" in u or "MapServer" in u):
                feature_layer_url = u
                break

        # If not found, try direct dataset id "layers" endpoint:
        if not feature_layer_url and dataset_id:
            layers_url = f"{base_api}/{dataset_id}/layers"
            lr = _http_get(layers_url, timeout=25)
            if lr.status_code == 200:
                lj = lr.json()
                for el in lj.get("data", []):
                    attrs = el.get("attributes") or {}
                    u = attrs.get("url")
                    if u and ("FeatureServer" in u or "MapServer" in u):
                        feature_layer_url = u
                        break

        if not feature_layer_url:
            return None

        # Query the FeatureServer layer with geometry filter
        # Use ESRI JSON geometry.
        query_url = feature_layer_url.rstrip("/") + "/query"
        params = {
            "f": "json",
            "where": "1=1",
            "geometryType": "esriGeometryPolygon",
            "geometry": {"rings": [ring], "spatialReference": {"wkid": 4326}},
            "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": False,
            "resultRecordCount": min(2000, max(1, limit)),
        }

        # ArcGIS servers accept either JSON-string geometry or form fields; we'll send as form with JSON parts.
        pr = requests.post(
            query_url,
            data={
                **{k: (v if isinstance(v, str) else json_dumps(v)) for k, v in params.items()},
            },
            headers={"User-Agent": USER_AGENT},
            timeout=60,
        )
        if pr.status_code != 200:
            return None
        pj = pr.json()
        feats = pj.get("features") or []
        if not feats:
            return None

        out: List[Dict[str, str]] = []
        for f in feats[:limit]:
            attrs = f.get("attributes") or {}

            # Field names vary; try common ones
            site_addr = (
                attrs.get("SITE_ADDRESS")
                or attrs.get("SITUS")
                or attrs.get("SITUS_ADDRESS")
                or attrs.get("ADDRESS")
                or attrs.get("FULLADDR")
                or ""
            )
            owner = (
                attrs.get("OWNER_NAME")
                or attrs.get("OWNER")
                or attrs.get("OWNERNME")
                or ""
            )
            mail = (
                attrs.get("MAILING_ADDRESS")
                or attrs.get("MAIL_ADDR")
                or attrs.get("MAILADDRESS")
                or ""
            )

            # If we couldn't even get an address, skip
            site_addr = _clean(site_addr)
            if not site_addr:
                continue

            out.append(
                {
                    "address": site_addr,
                    "owner": _clean(owner),
                    "mailing_address": _clean(mail),
                    "phone": "",  # PBC datasets typically don't include phone
                    "source": "PBC_OPEN_DATA",
                }
            )

        # If this produced very little, treat it as failure so OSM can fill the gap
        if len(out) < 10:
            return None

        return out

    except Exception:
        return None


def json_dumps(x: Any) -> str:
    import json
    return json.dumps(x, separators=(",", ":"))

# ----------------------------
# 3) OSM Option A (Overpass) — improved completeness via tiling
# ----------------------------

def _overpass_addresses_in_bbox(south: float, west: float, north: float, east: float) -> List[Dict[str, Any]]:
    """
    Pull any element with addr:housenumber + addr:street within bbox.
    Includes nodes, ways, relations.
    """
    q = f"""
    [out:json][timeout:60];
    (
      nwr["addr:housenumber"]["addr:street"]({south},{west},{north},{east});
      nwr["addr:housenumber"]["addr:street:name"]({south},{west},{north},{east});
    );
    out center {OVERPASS_TILE_LIMIT};
    """
    r = _http_post(OVERPASS_URL, q, timeout=75)
    r.raise_for_status()
    return r.json().get("elements", []) or []

def _format_osm_address(tags: Dict[str, Any]) -> str:
    hn = _clean(tags.get("addr:housenumber"))
    st = _clean(tags.get("addr:street") or tags.get("addr:street:name"))
    city = _clean(tags.get("addr:city"))
    state = _clean(tags.get("addr:state"))
    postcode = _clean(tags.get("addr:postcode"))
    parts = []
    if hn or st:
        parts.append(" ".join([p for p in [hn, st] if p]).strip())
    if city:
        parts.append(city)
    # prefer "FL" style state if provided
    if state:
        parts.append(state)
    if postcode:
        parts.append(postcode)
    return ", ".join([p for p in parts if p]).strip()

def _pick_latlng(el: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    c = el.get("center")
    if c and "lat" in c and "lon" in c:
        return float(c["lat"]), float(c["lon"])
    return None

def _tile_bbox(south: float, west: float, north: float, east: float, step: float) -> List[Tuple[float, float, float, float]]:
    tiles = []
    lat = south
    while lat < north:
        lat2 = min(north, lat + step)
        lng = west
        while lng < east:
            lng2 = min(east, lng + step)
            tiles.append((lat, lng, lat2, lng2))
            lng = lng2
        lat = lat2
    return tiles

def _fetch_osm_in_polygon(latlngs: List[List[float]], limit: int) -> List[Dict[str, str]]:
    south, west, north, east = _bbox(latlngs)
    tiles = _tile_bbox(south, west, north, east, TILE_DEG)

    seen = set()
    out: List[Dict[str, str]] = []

    # small backoff between tiles to be nicer to Overpass
    for idx, (s, w, n, e) in enumerate(tiles):
        if len(out) >= limit:
            break

        try:
            elements = _overpass_addresses_in_bbox(s, w, n, e)
        except Exception:
            # brief backoff and continue
            time.sleep(0.8)
            continue

        for el in elements:
            tags = el.get("tags") or {}
            addr = _format_osm_address(tags)
            if not addr:
                continue

            ll = _pick_latlng(el)
            if not ll:
                continue
            lat, lng = ll

            # filter strictly to polygon (bbox tiling pulls extra)
            if not _point_in_poly(lat, lng, latlngs):
                continue

            key = addr.lower()
            if key in seen:
                continue
            seen.add(key)

            out.append(
                {
                    "address": addr,
                    "owner": "",
                    "mailing_address": "",
                    "phone": "",
                    "source": "OSM",
                }
            )
            if len(out) >= limit:
                break

        # light pacing
        if idx % 6 == 0:
            time.sleep(0.15)

    return out

# ----------------------------
# Public API used by app.py
# ----------------------------

def fetch_parcel_objects_in_polygon(latlngs: List[List[float]], limit: int = 80) -> List[Dict[str, str]]:
    """
    Returns list of dicts with keys:
      - address
      - owner
      - mailing_address
      - phone
    This is what your app.py expects.

    Behavior:
      1) If polygon is in Palm Beach County → try PBC Open Data (owner/mailing best chance)
      2) Always fall back to OSM to ensure we still get lots of addresses
    """
    if not latlngs or len(latlngs) < 3:
        return []

    limit = max(1, int(limit))

    # Try Palm Beach “PAPA-style” enrichment automatically
    if _is_palm_beach_county(latlngs):
        pbc = _try_pbc_open_data(latlngs, limit=limit)
        if pbc:
            # If PBC returns fewer than limit, top up with OSM
            if len(pbc) < limit:
                osm = _fetch_osm_in_polygon(latlngs, limit=limit - len(pbc))
                # dedupe by address
                seen = {x["address"].strip().lower() for x in pbc}
                for x in osm:
                    if x["address"].strip().lower() not in seen:
                        pbc.append(x)
                        seen.add(x["address"].strip().lower())
                        if len(pbc) >= limit:
                            break
            return pbc

    # Default: OSM
    return _fetch_osm_in_polygon(latlngs, limit=limit)
