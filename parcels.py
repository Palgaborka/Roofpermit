from __future__ import annotations

import math
import random
import time
from typing import Any, Dict, List, Tuple

import requests


# Public Overpass endpoint (you can swap if rate-limited)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _poly_string(latlngs: List[List[float]]) -> str:
    """
    Overpass 'poly' uses "lat lon lat lon ..." (space separated).
    Input is [[lat,lng],...]
    Ensures polygon is closed.
    """
    if not latlngs or len(latlngs) < 3:
        raise ValueError("Polygon needs at least 3 points")

    pts = [(float(p[0]), float(p[1])) for p in latlngs]

    # Close polygon if not closed
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    return " ".join(f"{lat:.6f} {lon:.6f}" for lat, lon in pts)


def _build_address(tags: Dict[str, Any]) -> str:
    """
    Build a single-line street address from OSM addr:* tags.
    """
    hn = (tags.get("addr:housenumber") or "").strip()
    street = (tags.get("addr:street") or "").strip()
    unit = (tags.get("addr:unit") or tags.get("addr:flats") or "").strip()
    city = (tags.get("addr:city") or "").strip()
    state = (tags.get("addr:state") or "").strip()
    postcode = (tags.get("addr:postcode") or "").strip()

    main = " ".join(x for x in [hn, street] if x).strip()
    if unit:
        # Keep unit simple
        # e.g. "123 Main St Unit 4"
        main = f"{main} Unit {unit}".strip()

    # We return just the street address as your scan expects.
    # (If you ever want full address lines, append city/state/zip here.)
    return main


def _clean_address(addr: str) -> str:
    """
    Basic normalization (keeps it lightweight; your app later also normalizes).
    """
    addr = (addr or "").replace(",", " ")
    addr = " ".join(addr.split()).strip()
    return addr


def _element_center(el: Dict[str, Any]) -> Tuple[float, float]:
    """
    Return (lat, lon) for an Overpass element using best available fields.
    """
    if "lat" in el and "lon" in el:
        return float(el["lat"]), float(el["lon"])
    if "center" in el and isinstance(el["center"], dict):
        c = el["center"]
        return float(c.get("lat", 0.0)), float(c.get("lon", 0.0))
    # Fallback
    return 0.0, 0.0


def fetch_parcel_objects_in_polygon(latlngs: List[List[float]], limit: int = 80) -> List[Dict[str, str]]:
    """
    Option A: Fetch address points/ways/relations from OpenStreetMap (Overpass) inside the polygon.
    Returns a list of dicts compatible with your app:
      { "address": "...", "owner": "", "mailing_address": "", "phone": "" }

    Notes:
    - OSM is great for addresses; not for owner/mailing/phone.
    - Overpass can be rate-limited; we do a couple retries with backoff.
    """
    limit = int(limit or 80)
    limit = max(1, min(limit, 5000))  # keep sane

    poly = _poly_string(latlngs)

    # Query: all nodes/ways/relations with BOTH addr:housenumber and addr:street within polygon
    # out center ensures ways/relations return a center point
    query = f"""
    [out:json][timeout:25];
    (
      nwr["addr:housenumber"]["addr:street"](poly:"{poly}");
    );
    out center;
    """

    headers = {
        "User-Agent": "RoofSpy/1.0 (contact: example@example.com)",
        "Accept": "application/json",
    }

    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            r = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers=headers,
                timeout=35,
            )
            # Overpass sometimes returns 429/504; treat non-200 as retryable
            if r.status_code != 200:
                raise RuntimeError(f"Overpass HTTP {r.status_code}: {r.text[:200]}")
            data = r.json()
            elements = data.get("elements", []) or []
            out: List[Dict[str, str]] = []
            seen = set()

            for el in elements:
                tags = el.get("tags") or {}
                addr = _clean_address(_build_address(tags))
                if not addr:
                    continue

                # Basic dedupe by normalized address
                key = addr.lower()
                if key in seen:
                    continue
                seen.add(key)

                lat, lon = _element_center(el)
                out.append(
                    {
                        "address": addr,
                        "owner": "",
                        "mailing_address": "",
                        "phone": "",
                        "lat": f"{lat:.6f}" if lat else "",
                        "lon": f"{lon:.6f}" if lon else "",
                    }
                )

                if len(out) >= limit:
                    break

            # Sort for stability (nice UX)
            out.sort(key=lambda x: x.get("address", ""))
            return out

        except Exception as e:
            last_err = e
            # Exponential backoff + jitter
            sleep_s = min(6.0, (2 ** (attempt - 1)) + random.uniform(0.2, 0.8))
            time.sleep(sleep_s)

    raise RuntimeError(f"Overpass failed after retries: {last_err}")
