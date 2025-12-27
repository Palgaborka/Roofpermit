from __future__ import annotations
from sqlalchemy.orm import Session
from datetime import datetime

from ..models import Permit
from ..normalize import clean_street_address, normalize_permit_type, normalize_status, is_roofing_permit
from ..adapters.arcgis_capecoral_permits import CapeCoralPermitsArcGISClient

SOURCE_CITY = "CAPE CORAL, FL"
SOURCE_PORTAL = "ARCGIS"

def upsert_permit(db: Session, *, record: dict) -> Permit:
    existing = db.query(Permit).filter(
        Permit.source_city == record["source_city"],
        Permit.source_portal == record["source_portal"],
        Permit.source_record_id == record["source_record_id"],
    ).one_or_none()

    now = datetime.utcnow()
    if existing:
        for k, v in record.items():
            setattr(existing, k, v)
        existing.last_seen_at = now
        return existing

    p = Permit(**record, first_seen_at=now, last_seen_at=now)
    db.add(p)
    return p

def ingest_capecoral(db: Session, days_back: int = 14) -> dict:
    client = CapeCoralPermitsArcGISClient()
    raw = client.fetch_recent(days_back=days_back)

    inserted_or_updated = 0
    roofing_count = 0

    for r in raw:
        if is_roofing_permit(r.permit_type):
            roofing_count += 1

        record = {
            "source_city": SOURCE_CITY,
            "source_portal": SOURCE_PORTAL,
            "source_record_id": r.source_record_id,
            "detail_url": r.detail_url,
            "address_raw": r.address,
            "address_clean": clean_street_address(r.address),
            "permit_type_raw": r.permit_type,
            "permit_type_normalized": normalize_permit_type(r.permit_type),
            "status_raw": r.status,
            "status_normalized": normalize_status(r.status),
            "filed_date": r.filed_date,
            "issued_date": r.issued_date,
            "final_date": r.final_date,
            "contractor_name": r.contractor,
        }

        upsert_permit(db, record=record)
        inserted_or_updated += 1

    db.commit()

    return {
        "source_city": SOURCE_CITY,
        "days_back": days_back,
        "total_seen": len(raw),
        "inserted_or_updated": inserted_or_updated,
        "roofing_matched": roofing_count,
    }
