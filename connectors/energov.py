from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

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
        # Some datasets can produce things like "LOT 12 ...", ignore those
        raise ValueError(f"No usable search input (missing house number): {raw}")

    house = m.group(1)
    rest = m.group(2).strip()

    # If rest is very short (e.g., "DIVISION") still OK
    # Normalize ordinals: "7TH" stays "7TH"
    # Normalize multiple spaces
    rest = _clean_spaces(rest)

    # If rest ends with something weird, still keep it
    # But try to clip after a plausible street suffix to avoid noise
    # Example: "800 DOUGLAS AVE REAR" -> keep "DOUGLAS AVE"
    suf = re.search(rf"\b{_STREET_SUFFIXES}\b", rest)
    if suf:
        # keep through the suffix token
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

        # Faster + less bandwidth
        _context = _browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="RoofSpy/1.0",
        )

        # Block heavy resources (speed)
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
@dataclass
class EnerGovConnector:
    portal_url: str  # the URL you stored in jurisdictions.portal_url

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

        # Hard timeouts (don’t hang forever)
        page.set_default_timeout(25000)

        try:
            # Go to portal search page
            page.goto(self.portal_url, wait_until="domcontentloaded")

            # EnerGov UIs differ slightly. We try common patterns.
            # Strategy:
            #  1) Ensure we’re on a permit search screen
            #  2) Find address search input(s)
            #  3) Enter house + street (or full) and search
            #  4) Parse first matching permit result rows for reroof/roof terms

            # Attempt to locate a general search input
            # (EnerGov often has a search box labeled "Address", "Search", etc.)
            # We'll try multiple selectors.
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

            # Some EnerGov builds have separate "Street No" and "Street Name" fields
            street_no_sel = None
            street_name_sel = None
            for sel in ['input[aria-label*="Street Number" i]', 'input[placeholder*="Street Number" i]', 'input[name*="streetno" i]']:
                if page.query_selector(sel):
                    street_no_sel = sel
                    break
            for sel in ['input[aria-label*="Street Name" i]', 'input[placeholder*="Street Name" i]', 'input[name*="streetname" i]']:
                if page.query_selector(sel):
                    street_name_sel = sel
                    break

            # Fill in the best-available fields
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
                # As a last resort, type into any visible input
                any_input = page.query_selector("input")
                if not any_input:
                    return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": ""}
                any_input.click()
                any_input.fill(f"{house_no} {street}")
                query_used = f"{house_no} {street}"

            # Click a Search button if present (common)
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

            # If no explicit button, press Enter
            if not clicked:
                page.keyboard.press("Enter")

            # Wait for results area to update
            # We avoid huge waits; if it doesn't load, we error clearly.
            time.sleep(1.0)

            # Now parse results.
            # We look for table rows / list items containing "ROOF" / "REROOF" / etc.
            roof_terms = ["ROOF", "REROOF", "RE-ROOF", "RE ROOF"]
            content = page.content().upper()

            if not any(t in content for t in roof_terms):
                # Not necessarily an error; could be no permits
                return {"roof_detected": False, "error": "", "query_used": query_used}

            # Try to find a permit number-like token
            permit_no = ""
            m = re.search(r"\b(?:PERMIT|PMT)\s*#?\s*([A-Z0-9\-]{6,})\b", content)
            if m:
                permit_no = m.group(1)

            # Try to extract a date (mm/dd/yyyy) near roof terms
            roof_date = ""
            dm = re.search(r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/(19|20)\d{2}\b", content)
            if dm:
                roof_date = dm.group(0)

            # Estimate roof years if we got a date
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

            # Type line: first roof-ish line
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
