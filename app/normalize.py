import re

ROOF_TYPE_KEYWORDS = [
    "ROOFING - RESIDENTIAL",
    "ROOFING - COMMERCIAL",
    "ROOFING RESIDENTIAL",
    "ROOFING COMMERCIAL",
    "ROOFING",
    "REROOF",
    "RE-ROOF",
    "RE ROOF",
    "ROOF REPLAC",
    "ROOF",
]

def clean_street_address(addr: str | None) -> str | None:
    if not addr:
        return None
    addr = addr.replace(",", " ")
    addr = " ".join(addr.split()).strip()
    return addr.upper()

def is_roofing_permit(permit_type_raw: str | None) -> bool:
    if not permit_type_raw:
        return False
    t = permit_type_raw.upper()
    return any(k in t for k in ROOF_TYPE_KEYWORDS)

def normalize_permit_type(permit_type_raw: str | None) -> str | None:
    if not permit_type_raw:
        return None
    t = permit_type_raw.upper()
    if "REROOF" in t or "RE-ROOF" in t or "RE ROOF" in t or "REPLAC" in t:
        return "ROOF_REPLACEMENT"
    if "REPAIR" in t:
        return "ROOF_REPAIR"
    if "ROOF" in t:
        return "ROOFING_OTHER"
    return "OTHER"

def normalize_status(status_raw: str | None) -> str | None:
    if not status_raw:
        return None
    s = status_raw.upper()
    if any(x in s for x in ["FINAL", "CLOSED", "COMPLET"]):
        return "CLOSED"
    if any(x in s for x in ["ISSUED", "APPROV", "PERMIT"]):
        return "ISSUED"
    if any(x in s for x in ["OPEN", "IN REVIEW", "PENDING", "SUBMIT"]):
        return "OPEN"
    return "UNKNOWN"
