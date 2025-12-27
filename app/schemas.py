from pydantic import BaseModel
from datetime import datetime

class PermitOut(BaseModel):
    id: int
    source_city: str
    source_portal: str
    source_record_id: str
    detail_url: str | None = None

    address_raw: str | None = None
    address_clean: str | None = None

    permit_type_raw: str | None = None
    permit_type_normalized: str | None = None

    status_raw: str | None = None
    status_normalized: str | None = None

    filed_date: datetime | None = None
    issued_date: datetime | None = None
    final_date: datetime | None = None

    contractor_name: str | None = None

    first_seen_at: datetime
    last_seen_at: datetime

    class Config:
        from_attributes = True
