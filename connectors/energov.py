from __future__ import annotations

import re
import threading
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
# Playwright singleton
# -----------------------------
_pw_lock = threading.Lock()
_pw = None
_browser = None
_context = None

def _get_context():
    global _pw, _browser, _context
    with _pw_lock:
        if _context:
            return _context

        _pw = sync_playwright().start()
        _browser = _pw.chromium.launch(headless=True)
        _context = _browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="RoofSpy/1.0",
        )

        def block(route, request):
            if request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()

        _context.route("**/*", block)
        return _context


# -----------------------------
# EnerGov Connector
# -----------------------------
def _extract_url(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    url = getattr(obj, "portal_url", "")
    if not isinstance(url, str):
        raise ValueError("Invalid EnerGov portal URL")
    return url


@dataclass
class EnerGovConnector:
    portal: Any

    def __post_init__(self):
        self.portal_url = _extract_url(self.portal)

    def search_roof(self, address: str) -> Dict[str, Any]:
        try:
            house, street = parse_address_for_search(address)
        except Exception as e:
            return {"roof_detected": False, "error": str(e), "query_used": ""}

        ctx = _get_context()
        page = ctx.new_page()
        page.set_default_timeout(30000)

        try:
            # ---- CRITICAL FIX ----
            page.goto(self.portal_url, wait_until="domcontentloaded")
            try:
                page.wait_for_url("**/search*", timeout=20000)
            except Exception:
                pass
            page.wait_for_selector("input", timeout=20000)
            time.sleep(0.6)

            # Find input
            box = page.query_selector("input")
            if not box:
                return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": ""}

            box.fill(f"{house} {street}")
            page.keyboard.press("Enter")
            time.sleep(1.2)

            content = page.content().upper()
            roof_terms = ["ROOF", "REROOF", "RE-ROOF"]

            if not any(t in content for t in roof_terms):
                return {"roof_detected": False, "error": "", "query_used": f"{house} {street}"}

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
                "query_used": f"{house} {street}",
                "permit_no": permit,
                "type_line": "ROOF",
                "roof_date": date,
                "issued": "",
                "finalized": "",
                "applied": "",
                "roof_years": years,
                "is_20plus": is_20,
                "error": "",
            }

        except PWTimeoutError:
            return {"roof_detected": False, "error": "EnerGov timeout", "query_used": f"{house} {street}"}
        except Exception as e:
            return {"roof_detected": False, "error": f"EnerGov error: {e}", "query_used": f"{house} {street}"}
        finally:
            page.close()
