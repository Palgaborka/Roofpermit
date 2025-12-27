from __future__ import annotations
from typing import Dict

from connectors.base import PermitConnector
from utils import clean_street_address
from scanner import EnerGovScanner

class EnerGovConnector(PermitConnector):
    def search_roof(self, address: str) -> Dict:
        addr = clean_street_address(address)
        with EnerGovScanner(fast_mode=False, portal_url=self.j.portal_url) as s:
            return s.search_address(addr)
