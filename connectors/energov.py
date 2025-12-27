from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# -----------------------------
# Address parsing (robust)
# -----------------------------
_STREET_SUFFIXES = r"(?:ALY|AVE|BLVD|CIR|CT|DR|HWY|LN|LOOP|PKWY|PL|PLZ|RD|RUN|SQ|ST|TER|TRL|WAY|BLDG|BYP|CSWY|EXPY|FWY)"
_UNIT_MARKERS = r"(?:APT|UNIT|STE|SUITE|#)"

def _clean_spaces(s: str) -> str:
    s = (s or "").replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_address_for_search(raw: str) -> Tuple[str, str]:
    """
    Returns (house_number, street_name) for EnerGov address search.
    Never returns empty for common inputs like:
      "807 7TH ST", "801 DIVISION", "800 DOUGLAS AVE", "815 7TH ST", etc.
    """
    s = _clean_spaces(raw).upper()
    if not s:
        raise ValueError("No usable search input (empty address)")

    # Remove city/state/zip if someone passes them in
    # e.g. "807 7TH ST WEST PALM BEACH FL 33401"
    s = re.sub(r"\bFL\b.*$", "", s).strip()

    # Strip unit markers: "123 MAIN ST APT 4", "123 MAIN ST #4"
    s = re.sub(rf"\b{_UNIT_MARKERS}\b.*$", "", s).strip()

    # Common pattern: number + rest
    m = re.match(r"^(\d+)\s+(.*)$", s)
    if not m:
        raise ValueError(f"No usable search input (missing house number): {raw}")

    house = m.group(1)
    rest = m.group(2).strip()

    rest = _clean_spaces(rest)

    # Clip after suffix token if present
    suf = re.search(rf"\b{_STREET_SUFFIXES}\b", rest)
    if suf:
        tokens = rest.split()
        cut_idx = 0
        for i, t in enumerate(tokens):
            if re.fullmatch(_STREET_SUFFIXES, t):
                cut_idx = i
                break
        rest = " ".join(tokens[: cut_idx + 1]).strip()

    if not rest:
        raise ValueError(f"No usable search input (no street name): {raw}")

    return house, rest


# -----------------------------
# Playwright singleton (speed)
# -----------------------------
_pw_lock = threading.Lock()
_pw = None
_browser = None
_context = None

def _get_context():
    global _pw, _browser, _context
    with _pw_lock:
        if _context is not None:
            return _context

        _pw = sync_playwright().start()
        _browser = _pw.chromium.launch(headless=True)

        _context = _browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="RoofSpy/1.0",
        )

        def route_handler(route, request):
            rt = request.resource_type
            if rt in ("image", "media", "font"):
                return route.abort()
            return route.continue_()

        _context.route("**/*", route_handler)
        return _context


# -----------------------------
# EnerGov connector
# -----------------------------
def _extract_portal_url(portal_or_jurisdiction: Any) -> str:
    """
    Accepts either:
      - a portal_url string
      - a Jurisdiction object with .portal_url
    Returns a clean string URL.
    """
    if isinstance(portal_or_jurisdiction, str):
        url = portal_or_jurisdiction.strip()
    else:
        url = getattr(portal_or_jurisdiction, "portal_url", "") or ""
        url = str(url).strip()

    if not url.startswith("http"):
        raise ValueError("EnerGovConnector requires a valid portal URL string (http/https)")
    return url


@dataclass
class EnerGovConnector:
    """
    IMPORTANT:
      Your get_connector() may pass a Jurisdiction object.
      This class safely converts it to a string portal_url so Playwright page.goto() never crashes.
    """
    portal: Any  # string URL OR Jurisdiction object

    def __post_init__(self):
        self.portal_url: str = _extract_portal_url(self.portal)

    def search_roof(self, address: str) -> Dict[str, Any]:
        """
        Returns:
          roof_detected: bool
          roof_date / roof_years / is_20plus
          permit_no / type_line
          issued/finalized/applied if available
          error on failure
        """
        # Parse address safely
        try:
            house_no, street = parse_address_for_search(address)
        except Exception as e:
            return {"roof_detected": False, "error": str(e), "query_used": ""}

        ctx = _get_context()
        page = ctx.new_page()

        page.set_default_timeout(25000)

        try:
            # ✅ FIX: This is always a STRING now
            page.goto(self.portal_url, wait_until="domcontentloaded")

            selectors = [
                'input[placeholder*="Address" i]',
                'input[aria-label*="Address" i]',
                'input[placeholder*="Search" i]',
                'input[type="search"]',
                'input[name*="address" i]',
            ]

            box = None
            for sel in selectors:
                try:
                    el = page.query_selector(sel)
                    if el:
                        box = el
                        break
                except Exception:
                    pass

            street_no_sel = None
            street_name_sel = None
            for sel in [
                'input[aria-label*="Street Number" i]',
                'input[placeholder*="Street Number" i]',
                'input[name*="streetno" i]',
            ]:
                if page.query_selector(sel):
                    street_no_sel = sel
                    break

            for sel in [
                'input[aria-label*="Street Name" i]',
                'input[placeholder*="Street Name" i]',
                'input[name*="streetname" i]',
            ]:
                if page.query_selector(sel):
                    street_name_sel = sel
                    break

            query_used = ""
            if street_no_sel and street_name_sel:
                page.fill(street_no_sel, house_no)
                page.fill(street_name_sel, street)
                query_used = f"{house_no} {street}"
            elif box:
                box.click()
                box.fill(f"{house_no} {street}")
                query_used = f"{house_no} {street}"
            else:
                any_input = page.query_selector("input")
                if not any_input:
                    return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": ""}
                any_input.click()
                any_input.fill(f"{house_no} {street}")
                query_used = f"{house_no} {street}"

            clicked = False
            for btn_sel in [
                'button:has-text("Search")',
                'button:has-text("SEARCH")',
                'button:has-text("Apply")',
                'button:has-text("Go")',
                'button[aria-label*="Search" i]',
            ]:
                try:
                    btn = page.query_selector(btn_sel)
                    if btn:
                        btn.click()
                        clicked = True
                        break
                except Exception:
                    pass

            if not clicked:
                page.keyboard.press("Enter")

            time.sleep(1.0)

            roof_terms = ["ROOF", "REROOF", "RE-ROOF", "RE ROOF"]
            content = page.content().upper()

            if not any(t in content for t in roof_terms):
                return {"roof_detected": False, "error": "", "query_used": query_used}

            permit_no = ""
            m = re.search(r"\b(?:PERMIT|PMT)\s*#?\s*([A-Z0-9\-]{6,})\b", content)
            if m:
                permit_no = m.group(1)

            roof_date = ""
            dm = re.search(r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/(19|20)\d{2}\b", content)
            if dm:
                roof_date = dm.group(0)

            roof_years = ""
            is_20plus = ""
            if roof_date:
                try:
                    import datetime as _dt
                    mm, dd, yy = roof_date.split("/")
                    d = _dt.date(int(yy), int(mm), int(dd))
                    yrs = (_dt.date.today() - d).days / 365.25
                    roof_years = f"{yrs:.1f}"
                    is_20plus = "True" if yrs >= 20.0 else "False"
                except Exception:
                    pass

            type_line = ""
            for t in roof_terms:
                if t in content:
                    type_line = t
                    break

            return {
                "roof_detected": True if roof_date or permit_no or type_line else False,
                "query_used": query_used,
                "permit_no": permit_no,
                "type_line": type_line,
                "roof_date": roof_date,
                "issued": "",
                "finalized": "",
                "applied": "",
                "roof_years": roof_years,
                "is_20plus": is_20plus,
                "error": "",
            }

        except PWTimeoutError:
            return {"roof_detected": False, "error": "EnerGov timeout loading/searching", "query_used": f"{house_no} {street}"}
        except Exception as e:
            return {"roof_detected": False, "error": f"EnerGov error: {type(e).__name__}: {e}", "query_used": f"{house_no} {street}"}
        finally:
            try:
                page.close()
            except Exception:
                pass
