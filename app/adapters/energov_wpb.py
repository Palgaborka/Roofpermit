from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional, Any
import requests

from ..settings import settings

@dataclass
class RawPermit:
    source_record_id: str
    address: str | None
    permit_type: str | None
    status: str | None
    filed_date: datetime | None
    issued_date: datetime | None
    final_date: datetime | None
    contractor: str | None
    detail_url: str | None

class EnerGovWPBClient:
    """
    West Palm Beach EnerGov ingestion client.

    TODO: Implement _fetch_records() after capturing the JSON endpoint(s) used by the site.
    Start by opening the portal, searching a known address, then in DevTools -> Network
    find the XHR request returning permit rows. Copy:
      - URL
      - headers (often includes tenant/app keys)
      - request body (filters, paging)
    """

    def __init__(self, session: Optional[requests.Session] = None):
        self.s = session or requests.Session()

    def fetch_recent(self, days_back: int) -> list[RawPermit]:
        since = datetime.utcnow() - timedelta(days=days_back)
        rows = list(self._fetch_records(since=since))
        return rows

    def _fetch_records(self, since: datetime) -> Iterable[RawPermit]:
        # ---- PLACEHOLDER IMPLEMENTATION ----
        # Raise a clear error so you don’t think ingestion is “working” when it isn’t.
        raise NotImplementedError(
            "EnerGov WPB API not wired yet. Capture the portal XHR endpoint in DevTools "
            "and implement _fetch_records()."
        )
