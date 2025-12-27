from __future__ import annotations

import datetime
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# ============================================================
# Playwright singleton (stable + fast)
# ============================================================
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
            viewport={"width": 1400, "height": 900},
            user_agent="RoofSpy/1.0",
        )

        # Block heavy assets
        def route_handler(route, request):
            if request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()

        _context.route("**/*", route_handler)
        return _context


# ============================================================
# Address parsing (WPB-safe)
# ============================================================
def _clean(s: str) -> str:
    s = (s or "").replace(",", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def parse_address(raw: str) -> str:
    """
    West Palm Beach EnerGov accepts full address strings.
    We normalize but do NOT over-trim.
    """
    s = _clean(raw).upper()
    s = re.sub(r"\bFL\b.*$", "", s)
    return s


# ============================================================
# EnerGov – WEST PALM BEACH ONLY
# ============================================================
@dataclass
class EnerGovConnector:
    """
    STRICTLY for WEST PALM BEACH EnerGov.
    Assumes a permit grid with a 'Roof date' column.
    """
    portal: Any  # Jurisdiction object OR string URL

    def __post_init__(self):
        if isinstance(self.portal, str):
            self.portal_url = self.portal
        else:
            self.portal_url = getattr(self.portal, "portal_url", "")

        if not self.portal_url.startswith("http"):
            raise ValueError("Invalid EnerGov portal URL")

    # --------------------------------------------------------
    # MAIN SEARCH
    # --------------------------------------------------------
    def search_roof(self, address: str) -> Dict[str, Any]:
        query_used = parse_address(address)

        ctx = _get_context()
        page = ctx.new_page()
        page.set_default_timeout(30000)

        try:
            # 1️⃣ Load portal
            page.goto(self.portal_url, wait_until="domcontentloaded")

            # EnerGov SPA routing
            try:
                page.wait_for_url("**/search*", timeout=20000)
            except Exception:
                pass

            # 2️⃣ Wait for search input
            page.wait_for_selector("input", timeout=20000)
            time.sleep(0.5)

            # 3️⃣ Enter address
            search_box = page.query_selector('input[type="search"], input[placeholder*="Address" i], input')
            if not search_box:
                return self._error("No search input found", query_used)

            search_box.click()
            search_box.fill(query_used)
            page.keyboard.press("Enter")

            # 4️⃣ Wait for results grid
            time.sleep(2.0)

            # ====================================================
            # IMPORTANT PART: ROW-LEVEL PARSING
            # ====================================================
            # WPB EnerGov uses row-based permit cards / rows
            rows = page.query_selector_all(
                '[role="row"], .mat-row, .permit-row, tr'
            )

            roofing_rows: List[Dict[str, Any]] = []

            for row in rows:
                try:
                    text = row.inner_text().upper()
                except Exception:
                    continue

                if not any(k in text for k in ("ROOF", "REROOF", "RE-ROOF")):
                    continue

                # --- Extract Roof Date (column-based) ---
                date = self._extract_roof_date_from_row(row)
                if not date:
                    continue

                permit_no = self._extract_permit_no(text)

                roofing_rows.append({
                    "roof_date": date,
                    "permit_no": permit_no,
                    "text": text,
                })

            if not roofing_rows:
                return {
                    "roof_detected": False,
                    "query_used": query_used,
                    "error": "NO_ROOF_PERMIT_FOUND",
                }

            # 5️⃣ Pick MOST RECENT roofing permit
            roofing_rows.sort(key=lambda r: r["roof_date"], reverse=True)
            best = roofing_rows[0]

            roof_date = best["roof_date"]
            yrs = (datetime.date.today() - roof_date).days / 365.25

            return {
                "roof_detected": True,
                "query_used": query_used,
                "permit_no": best["permit_no"] or "",
                "type_line": "ROOF",
                "roof_date": roof_date.strftime("%m/%d/%Y"),
                "issued": "",
                "finalized": "",
                "applied": "",
                "roof_years": f"{yrs:.1f}",
                "is_20plus": "True" if yrs >= 20 else "False",
                "error": "",
            }

        except PWTimeoutError:
            return self._error("EnerGov timeout", query_used)
        except Exception as e:
            return self._error(f"EnerGov error: {e}", query_used)
        finally:
            try:
                page.close()
            except Exception:
                pass

    # --------------------------------------------------------
    # HELPERS
    # --------------------------------------------------------
    def _extract_roof_date_from_row(self, row) -> Optional[datetime.date]:
        """
        West Palm Beach rows contain a visible 'Roof date' cell.
        We scan child cells, not the whole page.
        """
        try:
            cells = row.query_selector_all("td, div, span")
        except Exception:
            return None

        for c in cells:
            try:
                t = c.inner_text().strip()
            except Exception:
                continue

            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", t):
                try:
                    m, d, y = t.split("/")
                    return datetime.date(int(y), int(m), int(d))
                except Exception:
                    continue
        return None

    def _extract_permit_no(self, text: str) -> str:
        m = re.search(r"\b[A-Z]{0,3}\d{4,}-?\d*\b", text)
        return m.group(0) if m else ""

    def _error(self, msg: str, query: str) -> Dict[str, Any]:
        return {
            "roof_detected": False,
            "query_used": query,
            "error": msg,
        }
