from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

ENERGOV_SEARCH_URL = "https://westpalmbeachfl-energovpub.tylerhost.net/apps/selfservice/WestPalmBeachFLProd#/search?m=2&ps=10&pn=1&em=true"

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

FUTURE_CUTOFF = datetime.now() + timedelta(days=1)

def clean_street_address(addr: str) -> str:
    addr = (addr or "").replace(",", " ")
    addr = " ".join(addr.split()).strip()
    return addr

def address_variants(street_only: str) -> List[str]:
    s = clean_street_address(street_only)
    if not s:
        return []
    # keep variants broad; connector/jurisdiction controls portal
    return [s, f"{s}, FL", f"{s} Florida"]

def norm(s: str) -> str:
    return (s or "").upper().replace("–", "-").strip()

def parse_date(token: Optional[str]) -> Optional[datetime]:
    if not token:
        return None
    token = token.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(token, fmt)
        except Exception:
            pass
    return None

def valid_date(d: Optional[datetime]) -> Optional[datetime]:
    if not d:
        return None
    if d > FUTURE_CUTOFF:
        return None
    return d

def roof_age_years(d: Optional[datetime]) -> Optional[float]:
    if not d:
        return None
    return (datetime.now() - d).days / 365.25

def extract_field(block_text: str, label: str) -> Optional[str]:
    m = re.search(rf"(?i)\b{label}\b\s*:?\s*(\d{{1,2}}/\d{{1,2}}/\d{{2,4}})", block_text)
    return m.group(1) if m else None

def extract_type_line(block_lines: List[str]) -> str:
    for line in block_lines:
        if re.match(r"(?i)^\s*Type\s*:?\s+", line):
            return line.strip()
    return ""

def block_is_roof(type_line: str, block_text: str) -> bool:
    t = norm(type_line)
    if any(k in t for k in ROOF_TYPE_KEYWORDS):
        return True
    bt = norm(block_text)
    return any(k in bt for k in ROOF_TYPE_KEYWORDS)

def parse_permit_blocks_from_text(page_text: str) -> List[Dict[str, Any]]:
    if not page_text:
        return []
    txt = page_text.replace("\r\n", "\n")

    hits = [m.start() for m in re.finditer(r"(?i)Permit Number\s*:?", txt)]
    if not hits:
        return []
    hits.append(len(txt))

    raw_blocks = [txt[hits[i]:hits[i + 1]].strip() for i in range(len(hits) - 1)]
    raw_blocks = [b for b in raw_blocks if b]

    parsed: List[Dict[str, Any]] = []
    for blk in raw_blocks:
        lines = [ln.strip() for ln in blk.splitlines() if ln.strip()]

        m_perm = re.search(r"(?i)Permit Number\s*:?\s*([A-Za-z0-9-]+)", blk)
        permit_no = m_perm.group(1) if m_perm else ""

        type_line = extract_type_line(lines)
        issued = parse_date(extract_field(blk, "Issued Date"))
        finalized = parse_date(extract_field(blk, "Finalized Date"))
        applied = parse_date(extract_field(blk, "Applied Date"))

        parsed.append({
            "permit_no": permit_no,
            "type_line": type_line,
            "issued_date": issued,
            "finalized_date": finalized,
            "applied_date": applied,
            "raw": blk,
        })
    return parsed

@dataclass
class LeadRow:
    address: str
    jurisdiction: str = ""          # NEW: selected jurisdiction name
    owner: str = ""                 # NEW
    mailing_address: str = ""       # NEW
    phone: str = ""                 # NEW
    query_used: str = ""
    permit_no: str = ""
    type_line: str = ""
    roof_date_used: str = ""
    issued: str = ""
    finalized: str = ""
    applied: str = ""
    roof_years: str = ""
    is_20plus: str = ""
    status: str = ""
    seconds: str = ""
