from __future__ import annotations

import datetime as _dt
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# Persisted storage for debug artifacts
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# Playwright singleton
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

        def route_handler(route, request):
            if request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()

        _context.route("**/*", route_handler)
        return _context


# ============================================================
# WPB URL normalizer
# ============================================================
_WPB_SEARCH_FRAGMENT = "/search?m=2&ps=10&pn=1&em=true"

def _ensure_wpb_search_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url
    p = urlparse(url)
    frag = p.fragment or ""
    if "search?m=" in frag.lower():
        return url
    # If fragment contains search but no params -> force params
    if "search" in frag.lower() or frag.strip() == "":
        frag = _WPB_SEARCH_FRAGMENT
        return urlunparse((p.scheme, p.netloc, p.path, p.params, p.query, frag))
    return url


# ============================================================
# Address normalization
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
# Extract helpers
# ============================================================
_DATE_RE = re.compile(r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/(19|20)\d{2}\b")
_PERMIT_RE = re.compile(r"\b[A-Z]{0,4}\d{3,}(?:-\d+)?\b")

_ROOF_TERMS = (
    "REROOF", "RE-ROOF", "RE ROOF",
    "ROOF REPLAC", "ROOF REPLACE",
    "ROOFING",
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
# EnerGov Connector (WPB focused + debug)
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
            # 1) Load
            page.goto(self.portal_url, wait_until="domcontentloaded")
            time.sleep(0.8)

            # 2) Wait for any visible enabled input
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
            time.sleep(0.6)

            # 3) Pick first visible enabled input
            search_box = None
            loc = page.locator("input")
            for i in range(min(loc.count(), 15)):
                el = loc.nth(i)
                try:
                    if el.is_visible() and el.is_enabled():
                        search_box = el
                        break
                except Exception:
                    continue

            if search_box is None:
                return {"roof_detected": False, "error": "EnerGov page: no input fields found", "query_used": query_used}

            # 4) Search
            search_box.click()
            search_box.fill(query_used)
            page.keyboard.press("Enter")

            # 5) Give results time to load
            time.sleep(2.0)

            # ------------------------------------------------------------
            # RESULTS CONTAINER DISCOVERY (more robust, less assumptions)
            # ------------------------------------------------------------
            # Instead of relying on a specific "Roof date" header,
            # we look for a container that has BOTH:
            #   - at least one roof term in text
            #   - at least one MM/DD/YYYY date in text
            # within a reasonably sized DOM subtree.
            #
            # This avoids the "results grid not found" issue on WPB.
            # ------------------------------------------------------------

            candidate_selectors = [
                "[role='grid']",
                "[role='table']",
                "table",
                ".mat-table",
                ".mat-mdc-table",
                ".results",
                ".search-results",
                ".content",
                "main",
                "body",
            ]

            grid = None
            for sel in candidate_selectors:
                try:
                    cand = page.locator(sel).first
                    if cand.count() == 0:
                        continue
                    # Try a quick content sample
                    sample = cand.inner_text(timeout=2500)
                    up = (sample or "").upper()
                    if any(t in up for t in _ROOF_TERMS) and _DATE_RE.search(up):
                        grid = cand
                        break
                except Exception:
                    continue

            if grid is None:
                # Debug dump for us to lock correct selectors
                png_path = DATA_DIR / "energov_debug.png"
                html_path = DATA_DIR / "energov_debug.html"
                try:
                    page.screenshot(path=str(png_path), full_page=True)
                except Exception:
                    pass
                try:
                    html_path.write_text(page.content(), encoding="utf-8")
                except Exception:
                    pass

                return {
                    "roof_detected": False,
                    "query_used": query_used,
                    "error": "EnerGov: results grid not found (debug saved: /app/data/energov_debug.png and /app/data/energov_debug.html)",
                }

            # 6) Scroll a bit to render virtual rows
            for _ in range(8):
                try:
                    grid.evaluate("el => { el.scrollBy(0, el.scrollHeight); }")
                except Exception:
                    try:
                        page.mouse.wheel(0, 1400)
                    except Exception:
                        pass
                time.sleep(0.25)

            # 7) Extract row-like items inside grid
            row_loc = grid.locator(
                "xpath=.//*[self::tr or @role='row' or contains(@class,'mat-row') or contains(@class,'mat-mdc-row') or contains(@class,'row')][.//text()]"
            )
            row_count = row_loc.count()

            # Fallback: div-like blocks
            if row_count == 0:
                row_loc = grid.locator("xpath=.//*[self::div or self::li][.//text()]")
                row_count = row_loc.count()

            if row_count == 0:
                return {"roof_detected": False, "query_used": query_used, "error": "EnerGov: results present but no rows detected"}

            roofing: List[Tuple[_dt.date, str, str]] = []

            max_rows = min(row_count, 160)
            for i in range(max_rows):
                try:
                    txt = row_loc.nth(i).inner_text(timeout=2200)
                except Exception:
                    continue
                if not txt:
                    continue

                up = txt.upper()
                if not any(t in up for t in _ROOF_TERMS):
                    continue

                dvals = [m.group(0) for m in _DATE_RE.finditer(up)]
                dates = [_parse_date(dv) for dv in dvals]
                dates = [d for d in dates if d is not None]
                if not dates:
                    continue

                best_date = max(dates)
                permit_no = _extract_permit_no(up)
                type_line = "REROOF" if ("REROOF" in up or "RE-ROOF" in up or "RE ROOF" in up) else "ROOF"
                roofing.append((best_date, permit_no, type_line))

            if not roofing:
                return {"roof_detected": False, "query_used": query_used, "error": "NO_ROOF_PERMIT_FOUND"}

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
                "is_20plus": "True" if yrs >= 20 else "False",
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
