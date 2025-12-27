from __future__ import annotations

import random
import time
from typing import Any, Dict, List, Tuple

import requests


# ----------------------------
# Palm Beach County “Parcels and Property Details” Feature Layer (you provided)
# ----------------------------
PBC_FEATURE_LAYER = "https://services1.arcgis.com/ZWOoUZbtaYePLlPw/arcgis/rest/services/Parcels_and_Property_Details_WebMercator/FeatureServer/0"

# A simple bounding box check so we only hit PBC when the polygon is roughly in PBC.
# (Keeps Cape Coral / other counties fast.)
# Rough PBC bounds (not perfect, but good enough):
PBC_BOUNDS = {
    "min_lat": 26.25,
    "max_lat": 27.25,
    "min_lon": -80.95,
    "max_lon": -79.85,
}

# ----------------------------
# OSM Overpass fallback (statewide)
# ----------------------------
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

USER_AGENT = "RoofSpy/1.0"


# ----------------------------
# Shared helpers
# ----------------------------
def _clean(s: str) -> str:
    s = (s or "").replace(",", " ")
    s = " ".join(s.split()).strip()
    return s


def _poly_close(latlngs: List[List[float]]) -> List[Tuple[float, float]]:
    if not latlngs or len(latlngs) < 3:
        raise ValueError("Polygon needs at least 3 points")
    pts = [(float(p[0]), float(p[1])) for p in latlngs]
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return pts


def _bbox_from_poly(pts: List[Tuple[float, float]]) -> Tuple[float, float, float, float]:
    lats = [p[0] for p in pts]
    lons = [p[1] for p in pts]
    return min(lats), min(lons), max(lats), max(lons)  # south, west, north, east


def _centroid(latlngs: List[List[float]]) -> Tuple[float, float]:
    lat = sum(p[0] for p in latlngs) / max(1, len(latlngs))
    lon = sum(p[1] for p in latlngs) / max(1, len(latlngs))
    return float(lat), float(lon)


def _point_in_poly(lat: float, lon: float, poly: List[Tuple[float, float]]) -> bool:
    # Ray casting
    n = len(poly)
    inside = False
    j = n - 1
    for i in range(n):
        yi, xi = poly[i][0], poly[i][1]
        yj, xj = poly[j][0], poly[j][1]
        intersect = ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / ((yj - yi) if (yj - yi) != 0 else 1e-12) + xi
        )
        if intersect:
            inside = not inside
        j = i
    return inside


def _within_pbc(latlngs: List[List[float]]) -> bool:
    lat, lon = _centroid(latlngs)
    return (
        PBC_BOUNDS["min_lat"] <= lat <= PBC_BOUNDS["max_lat"]
        and PBC_BOUNDS["min_lon"] <= lon <= PBC_BOUNDS["max_lon"]
    )


# ----------------------------
# Palm Beach County provider (ArcGIS FeatureServer)
# ----------------------------
def _arcgis_query_polygon(latlngs: List[List[float]], result_offset: int, result_count: int) -> Dict[str, Any]:
    """
    Query ArcGIS FeatureServer with a polygon geometry.
    We send geometry in WGS84 (wkid 4326) and let the service handle it.
    """
    ring = [[float(p[1]), float(p[0])] for p in latlngs]  # [lon, lat]
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    geom = {
        "rings": [ring],
        "spatialReference": {"wkid": 4326},
    }

    # Request only the fields we actually use (much faster than outFields=*)
    out_fields = ",".join(
        [
            "PARID",
            "PARCEL_NUMBER",
            "OWNER_NAME1",
            "OWNER_NAME2",
            "SITE_ADDR_STR",
            "MUNICIPALITY",
            "STATE",
            "ZIP1",
            "ZIP2",
            "PADDR1",
            "PADDR2",
            "PADDR3",
        ]
    )

    params = {
        "f": "json",
        "where": "1=1",
        "geometryType": "esriGeometryPolygon",
        "geometry": _json_dumps(geom),
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields,
        "returnGeometry": "false",
        "resultOffset": str(int(result_offset)),
        "resultRecordCount": str(int(result_count)),
    }

    r = requests.post(
        f"{PBC_FEATURE_LAYER}/query",
        data=params,
        headers={"User-Agent": USER_AGENT},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ArcGIS HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def _json_dumps(x: Any) -> str:
    import json

    return json.dumps(x, separators=(",", ":"))


def _fetch_pbc_parcels(latlngs: List[List[float]], limit: int) -> List[Dict[str, str]]:
    """
    Returns rows with address + owner + mailing for Palm Beach County.
    """
    limit = max(1, min(int(limit), 5000))

    out: List[Dict[str, str]] = []
    seen = set()

    # ArcGIS hosted layers often allow 2000 per request.
    batch = min(2000, limit)
    offset = 0

    while len(out) < limit:
        j = _arcgis_query_polygon(latlngs, result_offset=offset, result_count=batch)

        if "error" in j:
            raise RuntimeError(f"ArcGIS error: {j.get('error')}")

        feats = j.get("features") or []
        if not feats:
            break

        for f in feats:
            attrs = f.get("attributes") or {}

            street = _clean(str(attrs.get("SITE_ADDR_STR") or ""))
            if not street:
                continue

            key = street.lower()
            if key in seen:
                continue
            seen.add(key)

            owner1 = _clean(str(attrs.get("OWNER_NAME1") or ""))
            owner2 = _clean(str(attrs.get("OWNER_NAME2") or ""))
            owner = owner1 if not owner2 else f"{owner1} / {owner2}".strip(" /")

            mailing_parts = [
                _clean(str(attrs.get("PADDR1") or "")),
                _clean(str(attrs.get("PADDR2") or "")),
                _clean(str(attrs.get("PADDR3") or "")),
            ]
            mailing = " ".join([p for p in mailing_parts if p]).strip()

            out.append(
                {
                    "address": street,          # IMPORTANT: keep street-only for your permit search
                    "owner": owner,
                    "mailing_address": mailing,
                    "phone": "",
                    "source": "PBC",
                }
            )

            if len(out) >= limit:
                break

        # pagination
        offset += len(feats)

        # If server returned fewer than batch, we're done
        if len(feats) < batch:
            break

    out.sort(key=lambda x: x.get("address", ""))
    return out


# ----------------------------
# OSM fallback provider (Overpass, tiled)
# ----------------------------
def _element_center(el: Dict[str, Any]) -> Tuple[float, float]:
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    c = el.get("center")
    if isinstance(c, dict) and "lat" in c and "lon" in c:
        return float(c["lat"]), float(c["lon"])
    return 0.0, 0.0


def _build_address(tags: Dict[str, Any]) -> str:
    full = _clean(tags.get("addr:full") or "")
    if full:
        return full

    hn = _clean(tags.get("addr:housenumber") or "")
    street = _clean(tags.get("addr:street") or "")
    place = _clean(tags.get("addr:place") or "")
    unit = _clean(tags.get("addr:unit") or tags.get("addr:flats") or "")

    main = " ".join(x for x in [hn, street or place] if x).strip()
    if unit:
        main = f"{main} Unit {unit}".strip()
    return main


def _tile_bbox_adaptive(
    south: float, west: float, north: float, east: float
) -> List[Tuple[float, float, float, float]]:
    lat_span = max(1e-9, north - south)
    lon_span = max(1e-9, east - west)
    area_scale = lat_span * lon_span

    if area_scale > 0.002:
        target_tiles = 64
    elif area_scale > 0.0007:
        target_tiles = 36
    elif area_scale > 0.00025:
        target_tiles = 25
    elif area_scale > 0.00008:
        target_tiles = 16
    else:
        target_tiles = 9

    aspect = lon_span / lat_span
    cols = max(1, int((target_tiles * aspect) ** 0.5))
    rows = max(1, int(target_tiles / cols))

    lat_step = lat_span / rows
    lon_step = lon_span / cols

    tiles = []
    for r in range(rows):
        for c in range(cols):
            s = south + r * lat_step
            n = south + (r + 1) * lat_step
            w = west + c * lon_step
            e = west + (c + 1) * lon_step
            tiles.append((s, w, n, e))

    random.shuffle(tiles)
    return tiles


def _overpass_query_bbox(south: float, west: float, north: float, east: float) -> str:
    return f"""
    [out:json][timeout:90][maxsize:1073741824];
    (
      nwr["addr:full"]({south},{west},{north},{east});
      nwr["addr:housenumber"]({south},{west},{north},{east});
    );
    out center qt;
    """


def _post_overpass(endpoint: str, query: str) -> Dict[str, Any]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    r = requests.post(endpoint, data={"data": query}, headers=headers, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Overpass HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def _fetch_osm_in_polygon(latlngs: List[List[float]], limit: int) -> List[Dict[str, str]]:
    limit = max(1, min(int(limit), 5000))

    poly = _poly_close(latlngs)
    south, west, north, east = _bbox_from_poly(poly)
    tiles = _tile_bbox_adaptive(south, west, north, east)

    results: List[Dict[str, str]] = []
    seen = set()

    endpoints = list(OVERPASS_ENDPOINTS)
    random.shuffle(endpoints)

    def add_candidate(addr: str, lat: float, lon: float):
        addr = _clean(addr)
        if not addr:
            return
        key = addr.lower()
        if key in seen:
            return
        if lat and lon and not _point_in_poly(lat, lon, poly):
            return
        seen.add(key)
        results.append(
            {
                "address": addr,
                "owner": "",
                "mailing_address": "",
                "phone": "",
                "source": "OSM",
            }
        )

    for idx, (s, w, n, e) in enumerate(tiles):
        if len(results) >= limit:
            break

        query = _overpass_query_bbox(s, w, n, e)
        last_err: Exception | None = None

        for attempt in range(1, 4):
            endpoint = endpoints[(idx + attempt - 1) % len(endpoints)]
            try:
                data = _post_overpass(endpoint, query)
                elements = data.get("elements", []) or []
                for el in elements:
                    tags = el.get("tags") or {}
                    addr = _build_address(tags)
                    if not addr:
                        continue
                    lat, lon = _element_center(el)
                    add_candidate(addr, lat, lon)
                    if len(results) >= limit:
                        break
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(min(6.0, (2 ** (attempt - 1)) + random.uniform(0.2, 0.8)))

        # If Overpass is failing early, fail loudly so you see it
        if last_err and len(results) < 10 and idx < 6:
            raise RuntimeError(f"Overpass tile query failed early: {last_err}")

        if idx % 6 == 0:
            time.sleep(0.10)

    results.sort(key=lambda x: x.get("address", ""))
    return results


# ----------------------------
# Public API called by app.py
# ----------------------------
def fetch_parcel_objects_in_polygon(latlngs: List[List[float]], limit: int = 80) -> List[Dict[str, str]]:
    """
    Best behavior:
      - If polygon is in Palm Beach County: use PBC ArcGIS (owners + mailing), and top up with OSM if needed
      - Else: OSM
    """
    limit = int(limit or 80)
    limit = max(1, min(limit, 5000))

    # Palm Beach County first
    if _within_pbc(latlngs):
        try:
            pbc = _fetch_pbc_parcels(latlngs, limit=limit)
            if pbc:
                # top-up with OSM if needed
                if len(pbc) < limit:
                    osm = _fetch_osm_in_polygon(latlngs, limit=limit - len(pbc))
                    seen = {x["address"].strip().lower() for x in pbc}
                    for x in osm:
                        k = x["address"].strip().lower()
                        if k not in seen:
                            pbc.append(x)
                            seen.add(k)
                            if len(pbc) >= limit:
                                break
                return pbc
        except Exception:
            # fall back to OSM if ArcGIS fails
            pass

    # Default: OSM
    return _fetch_osm_in_polygon(latlngs, limit=limit)
