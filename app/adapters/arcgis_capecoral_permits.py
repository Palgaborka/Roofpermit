from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
import requests


ARCGIS_LAYER_URL = "https://capeims.capecoral.gov/arcgis/rest/services/OpenData/OpenData/MapServer/1"


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
    detail_url: str | None  # optional; ArcGIS table doesn’t have a public “detail page” per record


def _dt_to_arcgis_ms(dt: datetime) -> int:
    # ArcGIS REST dates are typically epoch milliseconds
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _arcgis_ms_to_dt(ms: int | None) -> datetime | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


class CapeCoralPermitsArcGISClient:
    """
    Pulls Building Permits from Cape Coral OpenData ArcGIS REST service.
    Layer: /MapServer/1 (Table)
    """

    def __init__(self, session: Optional[requests.Session] = None):
        self.s = session or requests.Session()

    def fetch_recent(self, days_back: int = 14, page_size: int = 2000) -> list[RawPermit]:
        since = datetime.utcnow() - timedelta(days=days_back)
        return list(self._query_since(since=since, page_size=page_size))

    def _query_since(self, since: datetime, page_size: int) -> Iterable[RawPermit]:
        since_ms = _dt_to_arcgis_ms(since.replace(tzinfo=timezone.utc))

        # Use applydate as primary “recent” filter; fallback to lastchangedon if you want later
        where = f"applydate >= TIMESTAMP '{since.strftime('%Y-%m-%d %H:%M:%S')}'"

        out_fields = ",".join(
            [
                "Permit_Number",
                "permit_status",
                "applydate",
                "issuedate",
                "finalizedate",
                "permit_desc",
                "Permit_Type",
                "Work_Class",
                "Addr1",
                "Predir",
                "Addr2",
                "Addr3",
                "Street_Type",
                "Post_Dir",
                "Unit",
                "City",
                "State",
                "Zip",
                "Contractor",
                "Company_Name",
                "lastchangedon",
            ]
        )

        offset = 0
        while True:
            params = {
                "f": "json",
                "where": where,
                "outFields": out_fields,
                "orderByFields": "applydate DESC",
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "returnGeometry": "false",
            }

            r = self.s.get(f"{ARCGIS_LAYER_URL}/query", params=params, timeout=60)
            r.raise_for_status()
            data = r.json()

            feats = data.get("features", [])
            if not feats:
                break

            for feat in feats:
                a = feat.get("attributes", {}) or {}
                permit_no = (a.get("Permit_Number") or "").strip()
                if not permit_no:
                    continue

                # Build a human-ish address from fields
                parts = [
                    a.get("Addr1"),
                    a.get("Predir"),
                    a.get("Addr2"),
                    a.get("Street_Type"),
                    a.get("Post_Dir"),
                ]
                street = " ".join([p for p in parts if p and str(p).strip()])

                unit = a.get("Unit")
                if unit:
                    street = f"{street} #{unit}"

                city = a.get("City") or "Cape Coral"
                state = a.get("State") or "FL"
                zipc = a.get("Zip")

                addr = street
                if city or state or zipc:
                    tail = " ".join([x for x in [city, state, zipc] if x and str(x).strip()])
                    addr = f"{street}, {tail}".strip(", ").strip()

                filed = _arcgis_ms_to_dt(a.get("applydate"))
                issued = _arcgis_ms_to_dt(a.get("issuedate"))
                final = _arcgis_ms_to_dt(a.get("finalizedate"))

                # Use Permit_Type / Work_Class / permit_desc as “type text”
                permit_type = " / ".join([x for x in [a.get("Permit_Type"), a.get("Work_Class"), a.get("permit_desc")] if x])

                yield RawPermit(
                    source_record_id=permit_no,
                    address=addr or None,
                    permit_type=permit_type or None,
                    status=(a.get("permit_status") or None),
                    filed_date=filed,
                    issued_date=issued,
                    final_date=final,
                    contractor=(a.get("Contractor") or a.get("Company_Name") or None),
                    detail_url=None,
                )

            offset += len(feats)

            # If server tells us it exceeded transfer limit, keep paging; otherwise stop when short page
            if data.get("exceededTransferLimit") is not True and len(feats) < page_size:
                break
