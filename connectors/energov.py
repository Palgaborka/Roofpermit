from __future__ import annotations

import datetime as _dt
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

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

        # Block only heavy assets (keep scripts!)
        def route_handler(route, request):
            if request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()

        _context.route("**/*", route_handler)
        return _context


# ============================================================
# West Palm Beach EnerGov URL normalizer
# ============================================================
_WPB_SEARCH_FRAGMENT = "/search?m=2&ps=10&pn=1&em=true"

def _ensure_wpb_search_url(url: str) -> str:
    """
    WPB EnerGov is an SPA. If you load only '#/search' without params,
    some sessions won't render the permit search UI as expected.

    This function forces:
      ...#/search?m=2&ps=10&pn=1&em=true
    """
    url = (url or "").strip()
    if not url:
        return url

    p = urlparse(url)
    frag = p.fragment or ""

    # If fragment already contains /search with params, keep it
    if "search?m=" in frag.lower():
        return url

    # If fragment is exactly '/search' or contains 'search' without params, force params
    if "search" in frag.lower():
        # normalize to /search?... even if it was /search or /search&...
        frag = _WPB_SEARCH_FRAGMENT
        return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, frag))

    # If no fragment at all, append the correct one
    frag = _WPB_SEARCH_FRAGMENT
    return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, frag))


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
    s = re.sub(r"\bFL\b.*$", "", s).strip()
    s = re.sub(rf"\b{_UNIT_MARKERS}\b.*$", "", s).strip()
    return s


# ============================================================
# Row-level extract helpers
# ============================================================
_DATE_RE = re.compile(r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/(19|20)\d{2}\b")
_PERMIT_RE = re.compile(r"\b[A-Z]{0,4}\d{3,}(?:-\d+)?\b")

_ROOF_TERMS = (
    "REROOF", "RE-ROOF", "RE ROOF",
    "ROOF REPLAC", "ROOF REPLACE", "ROOFING",
    "ROOF",
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
# EnerGov Connector (WPB-focused)
# ============================================================
@dataclass
class EnerGovConnector:
    portal: Any  # Jurisdiction object OR string URL

    def __post_init__(self):
        if isinstance(self.portal, str):
            raw = self.portal.strip()
        else:
            raw = (getattr(self.portal, "portal_url", "") or "").strip()

        self.portal_url = _ensure_wpb_search_url(raw)

        if not self.portal_url.startswith("http"):
            raise ValueError("Invalid EnerGov portal URL")

    def search_roof(self, address: str) -> Dict[str, Any]:
        query_used = normalize_address(address)
        if not query_used:
            return {"roof_detected": False, "error": "Empty address", "query_used": ""}

        ctx = _get_context()
        page = ctx.new_page()
        page.set_default_timeout(45000)

        try:
            # 1) Load the *correct* search URL (forced params)
            page.goto(self.portal_url, wait_until="domcontentloaded")

            # 2) Wait for SPA to render inputs (this fixes "no input fields found")
            # We wait for ANY visible, enabled input.
            page.wait_for_function(
                """
                () => {
                  const inputs = Array.from(document.querySelectorAll('input'));
                  return inputs.some(i => {
                    const r = i.getBoundingClientRect();
                    const visible = r.width > 0 && r.height > 0;
                    const notHidden = i.type !== 'hidden';
                    const notDisabled = !i.disabled;
                    return visible && notHidden && notDisabled;
                  });
                }
                """,
                timeout=35000,
            )

            # Small settle for Angular rendering
            time.sleep(0.6)

            # 3) Pick best search input
            # WPB EnerGov often has multiple inputs; choose first visible.
            search_box = None
            for sel in [
                'input[type="search"]',
                'input[placeholder*="Search" i]',
                'input[placeholder*="Address" i]',
                'input[aria-label*="Search" i]',
                'input[aria-label*="Address" i]',
                'input',
            ]:
                loc = page.locator(sel)
                if loc.count() > 0:
                    for i in range(min(loc.count(), 12)):
                        el = loc.nth(i)
                        try:
                            if el.is_visible() and el.is_enabled():
                                search_box = el
                                break
                        except Exception:
                            continue
                if search_box is not None:
                    break

            if search_box is None:
                return {
                    "roof_detected": False,
                    "error": "EnerGov page: no input fields found",
                    "query_used": query_used,
                }

            # 4) Enter address and submit
            search_box.click()
            search_box.fill(query_used)
            page.keyboard.press("Enter")

            # 5) Wait for results to appear.
            # We anchor on "Roof date" header text (you confirmed it exists).
            # If it doesn’t show quickly, we still try to parse rows after a delay.
            try:
                page.wait_for_selector("text=/Roof\\s*date/i", timeout=25000)
            except Exception:
                pass

            time.sleep(1.2)

            # 6) Find a likely results container
            # Prefer a grid/table ancestor near "Roof date".
            grid = None
            try:
                hdr = page.locator("text=/Roof\\s*date/i").first
                cand = hdr.locator(
                    "xpath=ancestor::*[self::table or @role='grid' or @role='table' or contains(@class,'table') or contains(@class,'grid') or contains(@class,'mat-table') or contains(@class,'mat-mdc-table')][1]"
                )
                if cand.count() > 0:
                    grid = cand.first
            except Exception:
                grid = None

            if grid is None:
                # fallback to any table/grid role
                cand = page.locator("[role='grid'], [role='table'], table").first
                if cand.count() > 0:
                    grid = cand

            if grid is None:
                return {
                    "roof_detected": False,
                    "error": "EnerGov: results grid not found",
                    "query_used": query_used,
                }

            # 7) Scroll to force virtualization to render rows
            for _ in range(8):
                try:
                    grid.evaluate("el => { el.scrollBy(0, el.scrollHeight); }")
                except Exception:
                    try:
                        page.mouse.wheel(0, 1400)
                    except Exception:
                        pass
                time.sleep(0.30)

            # 8) Collect row-like elements from inside the grid
            row_loc = grid.locator(
                "xpath=.//*[self::tr or @role='row' or contains(@class,'mat-row') or contains(@class,'mat-mdc-row') or contains(@class,'row')][.//text()]"
            )
            row_count = row_loc.count()

            # As a secondary fallback, collect “cards/items”
            if row_count == 0:
                row_loc = grid.locator(
                    "xpath=.//*[contains(@class,'card') or contains(@class,'item') or contains(@class,'result')][.//text()]"
                )
                row_count = row_loc.count()

            if row_count == 0:
                return {
                    "roof_detected": False,
                    "error": "EnerGov: no result rows detected",
                    "query_used": query_used,
                }

            # 9) Parse rows: filter roofing + get row-scoped roof date
            roofing: List[Tuple[_dt.date, str, str]] = []  # (date, permit_no, type_line)

            max_rows = min(row_count, 140)
            for i in range(max_rows):
                try:
                    txt = row_loc.nth(i).inner_text(timeout=2500)
                except Exception:
                    continue

                if not txt:
                    continue

                up = txt.upper()

                if not any(k in up for k in _ROOF_TERMS):
                    continue

                # row-scoped dates only
                dvals = [m.group(0) for m in _DATE_RE.finditer(up)]
                row_dates = []
                for dv in dvals:
                    dd = _parse_date(dv)
                    if dd:
                        row_dates.append(dd)

                if not row_dates:
                    continue

                # In WPB, the "Roof date" is typically the key date in row;
                # if multiple, choose latest within row.
                best_date = max(row_dates)

                permit_no = _extract_permit_no(up)
                type_line = "REROOF" if ("REROOF" in up or "RE-ROOF" in up or "RE ROOF" in up) else "ROOF"
                roofing.append((best_date, permit_no, type_line))

            if not roofing:
                return {
                    "roof_detected": False,
                    "error": "NO_ROOF_PERMIT_FOUND",
                    "query_used": query_used,
                }

            # 10) Select most recent roofing permit for roof age
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
            return {
                "roof_detected": False,
                "error": "EnerGov timeout loading/searching",
                "query_used": query_used,
            }
        except Exception as e:
            return {
                "roof_detected": False,
                "error": f"EnerGov error: {type(e).__name__}: {e}",
                "query_used": query_used,
            }
        finally:
            try:
                page.close()
            except Exception:
                pass
