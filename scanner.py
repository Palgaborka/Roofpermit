from __future__ import annotations

import random
import time
from datetime import datetime
from typing import Dict, Any, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from utils import (
    ENERGOV_SEARCH_URL,
    address_variants,
    clean_street_address,
    parse_permit_blocks_from_text,
    block_is_roof,
    valid_date,
    roof_age_years,
)


class EnerGovScanner:
    """
    Playwright-based scanner designed for cloud containers.

    Now supports per-jurisdiction EnerGov portal URLs.
    """

    def __init__(self, fast_mode: bool = False, portal_url: Optional[str] = None):
        self.fast_mode = fast_mode
        self.portal_url = (portal_url or ENERGOV_SEARCH_URL).strip()

        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def __enter__(self):
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        self._context = self._browser.new_context(viewport={"width": 1400, "height": 900})
        self._page = self._context.new_page()
        self._page.goto(self.portal_url, wait_until="domcontentloaded", timeout=60000)
        self._page.wait_for_timeout(600)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass

    def _overlay_gone(self):
        # Overlay is intermittent; treat as best-effort
        try:
            self._page.wait_for_selector("#overlay", state="hidden", timeout=8000 if self.fast_mode else 20000)
        except Exception:
            pass

    def _find_input(self):
        # EnerGov uses multiple inputs; pick first visible enabled input
        inputs = self._page.locator("input")
        n = inputs.count()
        for i in range(min(n, 30)):
            el = inputs.nth(i)
            try:
                if el.is_visible():
                    return el
            except Exception:
                continue
        return None

    def _click_search(self, input_el):
        # Prefer the Search button if present
        btn = self._page.locator("#button-Search")
        try:
            if btn.count() > 0 and btn.first.is_visible():
                btn.first.click(timeout=6000)
                return
        except Exception:
            pass
        # Fallback: Enter
        input_el.press("Enter")

    def _wait_results_or_stable(self):
        """
        Wait until either:
        - body contains "Permit Number", OR
        - body stops changing for a few cycles (0 results / partial render)
        """
        t0 = time.time()
        last = ""
        stable = 0
        limit = 25 if self.fast_mode else 55

        while time.time() - t0 < limit:
            self._overlay_gone()
            try:
                txt = self._page.inner_text("body")
            except Exception:
                txt = ""

            if "Permit Number" in txt:
                return txt

            if txt == last and len(txt) > 50:
                stable += 1
            else:
                stable = 0
                last = txt

            if stable >= 3:
                return txt

            self._page.wait_for_timeout(400)

        raise PWTimeout("Timed out waiting for results/text stabilization")

    def _parse_best_roof(self, page_text: str) -> Dict[str, Any]:
        blocks = parse_permit_blocks_from_text(page_text)
        roof_blocks = [b for b in blocks if block_is_roof(b["type_line"], b["raw"])]

        if not roof_blocks:
            return {"roof_detected": False}

        def best_date(b):
            issued = valid_date(b["issued_date"])
            finalized = valid_date(b["finalized_date"])
            applied = valid_date(b["applied_date"])
            return issued or finalized or applied or datetime.min

        best = max(roof_blocks, key=best_date)

        issued_dt = valid_date(best["issued_date"])
        finalized_dt = valid_date(best["finalized_date"])
        applied_dt = valid_date(best["applied_date"])
        roof_dt = issued_dt or finalized_dt or applied_dt

        yrs = roof_age_years(roof_dt) if roof_dt else None

        return {
            "roof_detected": True,
            "permit_no": best["permit_no"] or "",
            "type_line": best["type_line"] or "",
            "issued": issued_dt.strftime("%m/%d/%Y") if issued_dt else "",
            "finalized": finalized_dt.strftime("%m/%d/%Y") if finalized_dt else "",
            "applied": applied_dt.strftime("%m/%d/%Y") if applied_dt else "",
            "roof_date": roof_dt.strftime("%m/%d/%Y") if roof_dt else "",
            "roof_years": yrs if yrs is not None else "",
            "is_20plus": (yrs >= 20) if yrs is not None else "",
        }

    def _refresh_portal(self):
        # Refresh SPA / recover from broken state
        try:
            self._page.goto(self.portal_url, wait_until="domcontentloaded", timeout=60000)
            self._page.wait_for_timeout(800)
        except Exception:
            pass

    def _search_once(self, query: str) -> Dict[str, Any]:
        self._overlay_gone()

        input_el = self._find_input()
        if input_el is None:
            return {"roof_detected": False, "error": "No usable search input"}

        try:
            input_el.click(timeout=4000)
        except Exception:
            pass

        # Clear + type
        try:
            input_el.fill("")
        except Exception:
            # fallback: ctrl+a delete
            try:
                input_el.press("Control+A")
                input_el.press("Backspace")
            except Exception:
                pass

        input_el.type(query, delay=15 if not self.fast_mode else 5)

        self._overlay_gone()
        self._click_search(input_el)

        page_text = self._wait_results_or_stable()
        return self._parse_best_roof(page_text)

    def search_address(self, street_only: str) -> Dict[str, Any]:
        # Keep existing behavior, just ensure clean address
        street_only = clean_street_address(street_only)

        variants = address_variants(street_only)
        if not variants:
            return {"roof_detected": False, "error": "Empty address"}

        last_err = ""
        for q in variants:
            try:
                res = self._search_once(q)
                if res.get("roof_detected"):
                    res["query_used"] = q
                    return res
                last_err = res.get("error", "")

            except PWTimeout as e:
                last_err = f"Timeout: {e}"
                self._refresh_portal()

            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                self._refresh_portal()

            # small jitter between variant attempts
            try:
                self._page.wait_for_timeout(int(150 + random.uniform(0, 250)))
            except Exception:
                pass

        return {"roof_detected": False, "error": last_err or "Unknown error"}
