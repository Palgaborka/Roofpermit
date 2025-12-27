from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# -----------------------------
# Address parsing
# -----------------------------
_STREET_SUFFIXES = r"(?:ALY|AVE|BLVD|CIR|CT|DR|HWY|LN|LOOP|PKWY|PL|PLZ|RD|RUN|SQ|ST|TER|TRL|WAY)"
_UNIT_MARKERS = r"(?:APT|UNIT|STE|SUITE|#)"

def _clean_spaces(s: str) -> str:
    s = (s or "").replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_address_for_search(raw: str) -> Tuple[str, str]:
    s = _clean_spaces(raw).upper()
    s = re.sub(r"\bFL\b.*$", "", s)
    s = re.sub(rf"\b{_UNIT_MARKERS}\b.*$", "", s)

    m = re.match(r"^(\d+)\s+(.*)$", s)
    if not m:
        raise ValueError("Invalid address")

    house = m.group(1)
    rest = m.group(2)

    suf = re.search(rf"\b{_STREET_SUFFIXES}\b", rest)
    if suf:
        tokens = rest.split()
        for i, t in enumerate(tokens):
            if re.fullmatch(_STREET_SUFFIXES, t):
                rest = " ".join(tokens[: i + 1])
                break

    return house, rest.strip()


# -----------------------------
# EnerGov Connector (THREAD SAFE)
# -----------------------------
def _extract_url(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    return getattr(obj, "portal_url", "")


@dataclass
class EnerGovConnector:
    portal: Any

    def __post_init__(self):
        self.portal_url = _extract_url(self.portal)
        if not self.portal_url.startswith("http"):
            raise ValueError("Invalid EnerGov portal URL")

    def search_roof(self, address: str) -> Dict[str, Any]:
        try:
            house, street = parse_address_for_search(address)
        except Exception as e:
            return {"roof_detected": False, "error": str(e), "query_used": ""}

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={"width": 1280, "height": 800},
                    user_agent="RoofSpy/1.0",
                )

                page = context.new_page()
                page.set_default_timeout(30000)

                # --- EnerGov SPA load ---
                page.goto(self.portal_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_url("**/search*", timeout=20000)
                except Exception:
                    pass

                page.wait_for_selector("input", timeout=20000)
                time.sleep(0.6)

                box = page.query_selector("input")
                if not box:
                    return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": ""}

                query = f"{house} {street}"
                box.fill(query)
                page.keyboard.press("Enter")
                time.sleep(1.2)

                content = page.content().upper()
                roof_terms = ["ROOF", "REROOF", "RE-ROOF"]

                if not any(t in content for t in roof_terms):
                    return {"roof_detected": False, "error": "NO_ROOF_PERMIT_FOUND", "query_used": query}

                permit = ""
                m = re.search(r"\b[A-Z]{0,3}\d{4,}-?\d*\b", content)
                if m:
                    permit = m.group(0)

                date = ""
                d = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", content)
                if d:
                    date = d.group(0)

                years = ""
                is_20 = ""
                if date:
                    import datetime
                    mm, dd, yy = date.split("/")
                    yrs = (datetime.date.today() - datetime.date(int(yy), int(mm), int(dd))).days / 365.25
                    years = f"{yrs:.1f}"
                    is_20 = "True" if yrs >= 20 else "False"

                return {
                    "roof_detected": True,
                    "query_used": query,
                    "permit_no": permit,
                    "type_line": "ROOF",
                    "roof_date": date,
                    "roof_years": years,
                    "is_20plus": is_20,
                    "error": "",
                }

        except PWTimeoutError:
            return {"roof_detected": False, "error": "EnerGov timeout", "query_used": f"{house} {street}"}
        except Exception as e:
            return {"roof_detected": False, "error": f"EnerGov error: {e}", "query_used": f"{house} {street}"}
