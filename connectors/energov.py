from __future__ import annotations
from typing import Dict

from connectors.base import PermitConnector
from utils import clean_street_address
from scanner import EnerGovScanner  # your existing working scanner

class EnerGovConnector(PermitConnector):
    """
    EnerGov connector (Tyler EnerGov public portal).

    NOTE:
    - This assumes your existing EnerGovScanner already works.
    - If your scanner has a hard-coded WPB URL, it will still work for WPB.
    - Next step (later): we’ll upgrade scanner to accept portal_url per jurisdiction.
    """
    def search_roof(self, address: str) -> Dict:
        addr = clean_street_address(address)

        # If your EnerGovScanner supports passing a portal url, uncomment:
        # with EnerGovScanner(portal_url=self.j.portal_url, fast_mode=False) as s:
        #     return s.search_address(addr)

        # Otherwise fall back to current scanner behavior (WPB working):
        with EnerGovScanner(fast_mode=False) as s:
            return s.search_address(addr)
