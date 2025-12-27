from __future__ import annotations

import random
import time
from typing import Any, Dict, List, Tuple

import requests


# Rotate between public Overpass endpoints (helps rate limits / slow nodes)
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

USER_AGENT = "RoofSpy/1.0"


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


def _point_in_poly(lat: float, lon: float, poly: List[Tuple[float, float]]) -> bool:
    """
    Ray casting algorithm. poly is [(lat,lon), ...] closed or not.
    """
    n = len(poly)
    if n < 3:
        return False
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


def _element_center(el: Dict[str, Any]) -> Tuple[float, float]:
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    c = el.get("center")
    if isinstance(c, dict) and "lat" in c and "lon" in c:
        return float(c["lat"]), float(c["lon"])
    return 0.0, 0.0


def _build_address(tags: Dict[str, Any]) -> str:
    """
    Prefer addr:full if present, else housenumber + street/place.
    """
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


def _post_overpass(endpoint: str, query: str) -> Dict[str, Any]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    r = requests.post(endpoint, data={"data": query}, headers=headers, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Overpass HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def _tile_bbox_adaptive(
    south: float, west: float, north: float, east: float
) -> Tuple[List[Tuple[float, float, float, float]], int]:
    """
    Adaptive tiling based on bbox size to avoid Overpass truncation.
    Returns (tiles, max_tiles) for debugging/awareness.
    """
    lat_span = max(1e-9, north - south)
    lon_span = max(1e-9, east - west)
    area_scale = lat_span * lon_span

    # Heuristic: larger bbox => more tiles
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

    # Make roughly square cells
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
    return tiles, len(tiles)


def _overpass_query_bbox(south: float, west: float, north: float, east: float) -> str:
    """
    Query addresses as n/w/r:
      - addr:full
      - OR addr:housenumber (often paired with street/place)
    """
    return f"""
    [out:json][timeout:90][maxsize:1073741824];
    (
      nwr["addr:full"]({south},{west},{north},{east});
      nwr["addr:housenumber"]({south},{west},{north},{east});
    );
    out center qt;
    """


def fetch_parcel_objects_in_polygon(latlngs: List[List[float]], limit: int = 80) -> List[Dict[str, str]]:
    """
    OSM/Overpass address fetch:
      - tiles bbox to avoid truncation
      - supports addr:full + addr:housenumber
      - filters points back into polygon
      - dedupes by normalized address

    Returns:
      { "address": "...", "owner": "", "mailing_address": "", "phone": "", "lat":"", "lon":"" }
    """
    limit = int(limit or 80)
    limit = max(1, min(limit, 5000))

    poly = _poly_close(latlngs)
    south, west, north, east = _bbox_from_poly(poly)

    tiles, _ = _tile_bbox_adaptive(south, west, north, east)

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
        # keep only what is inside the original polygon
        if lat and lon and not _point_in_poly(lat, lon, poly):
            return
        seen.add(key)
        results.append({
            "address": addr,
            "owner": "",
            "mailing_address": "",
            "phone": "",
            "lat": f"{lat:.6f}" if lat else "",
            "lon": f"{lon:.6f}" if lon else "",
        })

    # Pull tiles until we hit limit
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

        # If we’re getting basically nothing AND tiles are failing, raise so you see it
        if last_err and len(results) < 10 and idx < 6:
            raise RuntimeError(f"Overpass tile query failed early: {last_err}")

        # Be nice to Overpass
        if idx % 6 == 0:
            time.sleep(0.10)

    results.sort(key=lambda x: x.get("address", ""))
    return results
