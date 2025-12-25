from __future__ import annotations
import json
from typing import List, Dict, Any
import requests

from utils import clean_street_address

WPB_PARCELS_QUERY = "https://wpbgisportal.wpb.org/server/rest/services/Parcel/Parcels_New/FeatureServer/0/query"

def polygon_to_esri_geometry(latlngs: List[List[float]]) -> Dict[str, Any]:
    ring = [[lng, lat] for (lat, lng) in latlngs]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return {"rings": [ring]}

def fetch_parcel_addresses_in_polygon(latlngs: List[List[float]], limit: int = 200) -> List[str]:
    """
    Returns STREET-ONLY addresses (no city/state/zip) to avoid garbage/mailing fields.
    """
    geom = polygon_to_esri_geometry(latlngs)
    out_fields = ",".join([
        "SITE_ADDR_STR",
        "STREET_NUMBER", "STREET_FRACTION", "PRE_DIR", "STREET_NAME", "STREET_SUFFIX_ABBR", "POST_DIR",
    ])

    results: List[str] = []
    offset = 0
    page_size = 2000

    while len(results) < limit:
        payload = {
            "f": "json",
            "where": "1=1",
            "geometry": json.dumps(geom),
            "geometryType": "esriGeometryPolygon",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": out_fields,
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": min(page_size, limit - len(results)),
        }

        r = requests.get(WPB_PARCELS_QUERY, params=payload, timeout=60)
        r.raise_for_status()
        data = r.json()

        feats = data.get("features", []) or []
        if not feats:
            break

        for f in feats:
            a = f.get("attributes", {}) or {}
            addr = clean_street_address((a.get("SITE_ADDR_STR") or "").strip())

            if not addr:
                parts = []
                sn = a.get("STREET_NUMBER")
                if sn is not None:
                    parts.append(str(sn))
                frac = (a.get("STREET_FRACTION") or "").strip()
                if frac:
                    parts.append(frac)
                pre = (a.get("PRE_DIR") or "").strip()
                if pre:
                    parts.append(pre)
                name = (a.get("STREET_NAME") or "").strip()
                if name:
                    parts.append(name)
                suf = (a.get("STREET_SUFFIX_ABBR") or "").strip()
                if suf:
                    parts.append(suf)
                post = (a.get("POST_DIR") or "").strip()
                if post:
                    parts.append(post)
                addr = clean_street_address(" ".join(parts))

            if addr:
                results.append(addr)
                if len(results) >= limit:
                    break

        if not data.get("exceededTransferLimit"):
            break
        offset += len(feats)

    dedup, seen = [], set()
    for x in results:
        x = clean_street_address(x)
        k = x.upper()
        if x and k not in seen:
            seen.add(k)
            dedup.append(x)
    return dedup
