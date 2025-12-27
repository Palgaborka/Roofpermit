from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class Jurisdiction:
    id: int
    state: str
    name: str
    system: str            # "energov", "accela", "arcgis", etc.
    portal_url: str
    active: int = 1

class PermitConnector:
    """
    Base interface for permit connectors.
    """
    def __init__(self, jurisdiction: Jurisdiction):
        self.j = jurisdiction

    def search_roof(self, address: str) -> Dict:
        """
        Return normalized result dict:

        {
          "roof_detected": bool,
          "query_used": str,
          "permit_no": str,
          "type_line": str,
          "roof_date": str,
          "issued": str,
          "finalized": str,
          "applied": str,
          "roof_years": float|str,
          "is_20plus": bool,
          "error": str
        }
        """
        raise NotImplementedError
