from __future__ import annotations

import datetime as _dt
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
# Address normalization (safe)
# ============================================================
_UNIT_MARKERS = r"(?:APT|UNIT|STE|SUITE|#)"
def _clean_spaces(s: str) -> str:
    s = (s or "").replace(",", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_address(raw: str) -> str:
    s = _clean_spaces(raw).upper()
    # Drop trailing "FL ...." if present
    s = re.sub(r"\bFL\b.*$", "", s).strip()
    # Drop unit markers
    s = re.sub(rf"\b{_UNIT_MARKERS}\b.*$", "", s).strip()
    return s


# ============================================================
# Helpers: date + permit extraction
# ============================================================
_DATE_RE = re.compile(r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/(19|20)\d{2}\b")
_PERMIT_RE = re.compile(r"\b[A-Z]{0,4}\d{3,}(?:-\d+)?\b")

_ROOF_TERMS = (
    "ROOF", "REROOF", "RE-ROOF", "RE ROOF",
    "ROOFING", "ROOF REPLAC", "REROOFING",
)

def _parse_date(mmddyyyy: str) -> Optional[_dt.date]:
    try:
        mm, dd, yy = mmddyyyy.split("/")
        return _dt.date(int(yy), int(mm), int(dd))
    except Exception:
        return None

def _extract_permit_no(text: str) -> str:
    m = _PERMIT_RE.search(text)
    return m.group(0) if m else ""


# ============================================================
# EnerGov Connector (West Palm Beach focused)
# ============================================================
@dataclass
class EnerGovConnector:
    portal: Any  # Jurisdiction object OR string URL

    def __post_init__(self):
        if isinstance(self.portal, str):
            self.portal_url = self.portal.strip()
        else:
            self.portal_url = (getattr(self.portal, "portal_url", "") or "").strip()

        if not self.portal_url.startswith("http"):
            raise ValueError("Invalid EnerGov portal URL")

    def search_roof(self, address: str) -> Dict[str, Any]:
        query_used = normalize_address(address)
        if not query_used:
            return {"roof_detected": False, "error": "Empty address", "query_used": ""}

        ctx = _get_context()
        page = ctx.new_page()
        page.set_default_timeout(35000)

        try:
            # 1) Load WPB search page
            page.goto(self.portal_url, wait_until="domcontentloaded")

            # Give SPA a moment
            time.sleep(0.8)

            # 2) Find a usable search box (EnerGov variations)
            search_box = None
            candidates = [
                'input[type="search"]',
                'input[placeholder*="Search" i]',
                'input[placeholder*="Address" i]',
                'input[aria-label*="Search" i]',
                'input[aria-label*="Address" i]',
                'input',
            ]
            for sel in candidates:
                try:
                    el = page.query_selector(sel)
                    if el:
                        # make sure visible/enabled-ish
                        try:
                            if el.is_visible():
                                search_box = el
                                break
                        except Exception:
                            search_box = el
                            break
                except Exception:
                    pass

            if not search_box:
                return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": query_used}

            # 3) Run search
            search_box.click()
            search_box.fill(query_used)
            page.keyboard.press("Enter")

            # 4) Wait for results: anchor on "Roof date" column header (you confirmed it exists)
            # If it never appears, the results grid didn't load / selector changed.
            try:
                page.wait_for_selector("text=/Roof\\s*date/i", timeout=25000)
            except Exception:
                # Sometimes the grid loads without that exact text; fallback to any rows
                pass

            time.sleep(1.0)

            # 5) Locate the grid/container near "Roof date"
            grid_handle = None
            try:
                # Try to find the header element
                hdr = page.locator("text=/Roof\\s*date/i").first
                # climb to a likely grid/table container
                grid = hdr.locator(
                    "xpath=ancestor::*[self::table or @role='grid' or @role='table' or contains(@class,'mat-table') or contains(@class,'mat-mdc-table') or contains(@class,'ag-root') or contains(@class,'grid')][1]"
                )
                if grid.count() > 0:
                    grid_handle = grid.first
            except Exception:
                grid_handle = None

            # Fallback: pick the biggest visible table/grid-ish thing
            if grid_handle is None:
                # try table
                t = page.locator("table").first
                if t.count() > 0:
                    grid_handle = t
                else:
                    # try any grid role
                    g = page.locator("[role='grid'], [role='table']").first
                    if g.count() > 0:
                        grid_handle = g

            if grid_handle is None:
                return {"roof_detected": False, "error": "EnerGov: results grid not found", "query_used": query_used}

            # 6) Force-render rows (virtualized grids need scroll)
            # We scroll a few times; if scroll fails, we still attempt a read.
            for _ in range(6):
                try:
                    grid_handle.evaluate("el => { el.scrollBy(0, el.scrollHeight); }")
                except Exception:
                    try:
                        page.mouse.wheel(0, 1200)
                    except Exception:
                        pass
                time.sleep(0.35)

            # 7) Collect row texts from within the grid (row-level parsing)
            row_loc = grid_handle.locator(
                "xpath=.//*[self::tr or @role='row' or contains(@class,'mat-row') or contains(@class,'mat-mdc-row') or contains(@class,'ag-row') or contains(@class,'row')]"
            )
            row_count = row_loc.count()

            # Sometimes the “row” elements are actually div cards; add fallback card selector
            card_loc = grid_handle.locator("xpath=.//*[contains(@class,'card') or contains(@class,'result') or contains(@class,'item')]")
            card_count = card_loc.count()

            texts: List[str] = []
            if row_count > 0:
                for i in range(min(row_count, 120)):  # cap
                    try:
                        txt = row_loc.nth(i).inner_text(timeout=2000)
                        txt = (txt or "").strip()
                        if txt:
                            texts.append(txt)
                    except Exception:
                        continue
            elif card_count > 0:
                for i in range(min(card_count, 120)):  # cap
                    try:
                        txt = card_loc.nth(i).inner_text(timeout=2000)
                        txt = (txt or "").strip()
                        if txt:
                            texts.append(txt)
                    except Exception:
                        continue

            if not texts:
                return {"roof_detected": False, "error": "EnerGov: no result rows detected", "query_used": query_used}

            # 8) Filter roofing permits + extract row-scoped roof date
            roofing: List[Tuple[_dt.date, str, str]] = []  # (date, permit_no, type_line)

            for t in texts:
                up = t.upper()

                if not any(k in up for k in _ROOF_TERMS):
                    continue

                # Row-scoped date: take the most relevant date in THIS ROW
                dates = _DATE_RE.findall(up)
                # findall returns tuples because of groups; re-find with finditer
                dvals = [m.group(0) for m in _DATE_RE.finditer(up)]
                row_dates: List[_dt.date] = []
                for dv in dvals:
                    dd = _parse_date(dv)
                    if dd:
                        row_dates.append(dd)

                if not row_dates:
                    continue

                # WPB has a “Roof date” column; usually only one date per row.
                # If multiple dates exist, choose the latest date in the row.
                best_date = max(row_dates)

                permit_no = _extract_permit_no(up)
                type_line = "REROOF" if ("REROOF" in up or "RE-ROOF" in up or "RE ROOF" in up) else "ROOF"

                roofing.append((best_date, permit_no, type_line))

            if not roofing:
                return {"roof_detected": False, "error": "NO_ROOF_PERMIT_FOUND", "query_used": query_used}

            # 9) Choose most recent roofing permit
            roofing.sort(key=lambda x: x[0], reverse=True)
            roof_date, permit_no, type_line = roofing[0]

            yrs = (_dt.date.today() - roof_date).days / 365.25
            return {
                "roof_detected": True,
                "query_used": query_used,
                "permit_no": permit_no or "",
                "type_line": type_line,
                "roof_date": roof_date.strftime("%m/%d/%Y"),
                "issued": "",
                "finalized": "",
                "applied": "",
                "roof_years": f"{yrs:.1f}",
                "is_20plus": "True" if yrs >= 20.0 else "False",
                "error": "",
            }

        except PWTimeoutError:
            return {"roof_detected": False, "error": "EnerGov timeout loading/searching", "query_used": query_used}
        except Exception as e:
            return {"roof_detected": False, "error": f"EnerGov error: {type(e).__name__}: {e}", "query_used": query_used}
        finally:
            try:
                page.close()
            except Exception:
                pass
