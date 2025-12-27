from __future__ import annotations

import json
import re
from typing import List, Dict, Any, Optional
import requests

from utils import clean_street_address

WPB_PARCELS_LAYER = "https://wpbgisportal.wpb.org/server/rest/services/Parcel/Parcels_New/FeatureServer/0"
WPB_PARCELS_QUERY = f"{WPB_PARCELS_LAYER}/query"


# ----------------------------
# Helpers: geometry + metadata
# ----------------------------

def polygon_to_esri_geometry(latlngs: List[List[float]]) -> Dict[str, Any]:
    ring = [[lng, lat] for (lat, lng) in latlngs]
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])
    return {"rings": [ring]}


_LAYER_FIELDS_CACHE: Optional[List[str]] = None


def get_layer_field_names() -> List[str]:
    """
    Reads ArcGIS layer metadata once and returns available field names.
    """
    global _LAYER_FIELDS_CACHE
    if _LAYER_FIELDS_CACHE is not None:
        return _LAYER_FIELDS_CACHE

    r = requests.get(WPB_PARCELS_LAYER, params={"f": "json"}, timeout=30)
    r.raise_for_status()
    data = r.json()

    fields = data.get("fields", []) or []
    names = []
    for f in fields:
        n = f.get("name")
        if n:
            names.append(str(n))
    _LAYER_FIELDS_CACHE = names
    return names


def pick_first(fields: List[str], candidates: List[str]) -> Optional[str]:
    """
    Return first existing field name, case-insensitive exact match.
    """
    fset = {f.upper(): f for f in fields}
    for c in candidates:
        if c.upper() in fset:
            return fset[c.upper()]
    return None


def pick_by_regex(fields: List[str], patterns: List[str]) -> Optional[str]:
    """
    Return first field matching any regex pattern (case-insensitive).
    """
    for pat in patterns:
        rx = re.compile(pat, re.IGNORECASE)
        for f in fields:
            if rx.search(f):
                return f
    return None


def join_parts(*parts: str) -> str:
    xs = []
    for p in parts:
        p = (p or "").strip()
        if p:
            xs.append(p)
    return ", ".join(xs)


def normalize_phone(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    digits = re.sub(r"\D+", "", s)
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return s


# ----------------------------
# Public functions
# ----------------------------

def fetch_parcel_objects_in_polygon(latlngs: List[List[float]], limit: int = 200) -> List[Dict[str, str]]:
    """
    Returns objects the app expects:
      [{"address": "...", "owner": "...", "mailing_address": "...", "phone": "..."}, ...]

    Uses the WPB ArcGIS layer and automatically detects field names for owner/mailing/phone if present.
    """
    fields = get_layer_field_names()

    # Address: prefer SITE_ADDR_STR, else construct from components (same as your old code)
    site_addr = pick_first(fields, ["SITE_ADDR_STR"])

    # Owner field guesses (varies by dataset)
    owner_field = (
        pick_first(fields, ["OWNER_NAME", "OWNER", "OWNERNM", "OWNER_NM"])
        or pick_by_regex(fields, [r"\bOWNER\b", r"OWNER_?NAME", r"OWN(ER)?_?NM"])
    )

    # Phone field guesses (rare, but we’ll attempt)
    phone_field = (
        pick_first(fields, ["PHONE", "OWNER_PHONE", "CONTACT_PHONE", "PHONE1", "PHONE_NUMBER"])
        or pick_by_regex(fields, [r"PHONE", r"TEL", r"TELEPHONE"])
    )

    # Mailing fields guesses
    mail_addr1 = pick_first(fields, ["MAIL_ADDR", "MAIL_ADDR1", "MAILING_ADDR", "MAILING_ADDRESS", "MAILADDRESS", "ADDRESS1"])
    mail_addr2 = pick_first(fields, ["MAIL_ADDR2", "MAIL_ADDR_2", "ADDRESS2"])
    mail_city  = pick_first(fields, ["MAIL_CITY", "MAILING_CITY", "CITY"])
    mail_state = pick_first(fields, ["MAIL_STATE", "MAILING_STATE", "STATE"])
    mail_zip   = pick_first(fields, ["MAIL_ZIP", "MAILING_ZIP", "ZIP", "ZIPCODE", "POSTAL", "POSTAL_CODE"])

    # Street components (fallback)
    street_number = pick_first(fields, ["STREET_NUMBER"])
    street_fraction = pick_first(fields, ["STREET_FRACTION"])
    pre_dir = pick_first(fields, ["PRE_DIR"])
    street_name = pick_first(fields, ["STREET_NAME"])
    street_suffix = pick_first(fields, ["STREET_SUFFIX_ABBR"])
    post_dir = pick_first(fields, ["POST_DIR"])

    # Build outFields list, only including what exists
    wanted = []
    for f in [
        site_addr,
        street_number, street_fraction, pre_dir, street_name, street_suffix, post_dir,
        owner_field,
        phone_field,
        mail_addr1, mail_addr2, mail_city, mail_state, mail_zip,
    ]:
        if f and f not in wanted:
            wanted.append(f)

    # Always include at least something to avoid ArcGIS errors
    if not wanted:
        wanted = ["*"]

    geom = polygon_to_esri_geometry(latlngs)

    results: List[Dict[str, str]] = []
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
            "outFields": ",".join(wanted),
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

            # Address
            addr = clean_street_address((a.get(site_addr) or "").strip()) if site_addr else ""
            if not addr:
                parts = []
                if street_number and a.get(street_number) is not None:
                    parts.append(str(a.get(street_number)))
                if street_fraction:
                    frac = (a.get(street_fraction) or "").strip()
                    if frac:
                        parts.append(frac)
                if pre_dir:
                    pre = (a.get(pre_dir) or "").strip()
                    if pre:
                        parts.append(pre)
                if street_name:
                    name = (a.get(street_name) or "").strip()
                    if name:
                        parts.append(name)
                if street_suffix:
                    suf = (a.get(street_suffix) or "").strip()
                    if suf:
                        parts.append(suf)
                if post_dir:
                    post = (a.get(post_dir) or "").strip()
                    if post:
                        parts.append(post)
                addr = clean_street_address(" ".join(parts))

            if not addr:
                continue

            # Owner
            owner = ""
            if owner_field:
                owner = (a.get(owner_field) or "").strip()

            # Phone
            phone = ""
            if phone_field:
                phone = normalize_phone(str(a.get(phone_field) or "").strip())

            # Mailing address
            m1 = (a.get(mail_addr1) or "").strip() if mail_addr1 else ""
            m2 = (a.get(mail_addr2) or "").strip() if mail_addr2 else ""
            mc = (a.get(mail_city) or "").strip() if mail_city else ""
            ms = (a.get(mail_state) or "").strip() if mail_state else ""
            mz = (a.get(mail_zip) or "").strip() if mail_zip else ""

            mailing = ""
            if any([m1, m2, mc, ms, mz]):
                line1 = " ".join([x for x in [m1, m2] if x])
                city_state_zip = " ".join([x for x in [mc, ms, mz] if x])
                if line1 and city_state_zip:
                    mailing = f"{line1}, {city_state_zip}"
                else:
                    mailing = line1 or city_state_zip

            results.append({
                "address": addr,
                "owner": owner,
                "mailing_address": mailing,
                "phone": phone,
            })

            if len(results) >= limit:
                break

        if not data.get("exceededTransferLimit"):
            break
        offset += len(feats)

    # Dedup by address
    dedup: List[Dict[str, str]] = []
    seen = set()
    for row in results:
        addr = clean_street_address(row.get("address", ""))
        key = addr.upper()
        if addr and key not in seen:
            seen.add(key)
            row["address"] = addr
            dedup.append(row)

    return dedup
