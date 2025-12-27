from .services.ingest_capecoral import ingest_capecoral
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import or_

from .db import Base, engine, get_db
from .models import Permit
from .schemas import PermitOut
from .scheduler import start_scheduler
from .services.ingest import ingest_wpb
from .settings import settings

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.app_name)

@app.on_event("startup")
def on_startup():
    start_scheduler()

@app.get("/health")
def health():
    return {"ok": True, "app": settings.app_name}

@app.post("/ingest/wpb")
def ingest_now(db: Session = Depends(get_db)):
    # manual trigger for testing
    return ingest_wpb(db, days_back=settings.ingest_days_back)

@app.get("/permits", response_model=list[PermitOut])
def list_permits(
    q: str | None = Query(default=None, description="Search permit # or address"),
    city: str | None = Query(default=None),
    roofing_only: bool = Query(default=True),
    status: str | None = Query(default=None, description="OPEN/ISSUED/CLOSED/UNKNOWN"),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    query = db.query(Permit)

    if city:
        query = query.filter(Permit.source_city == city)

    if roofing_only:
        query = query.filter(Permit.permit_type_normalized.in_(["ROOF_REPLACEMENT", "ROOF_REPAIR", "ROOFING_OTHER"]))

    if status:
        query = query.filter(Permit.status_normalized == status.upper())

    if q:
        qq = q.strip()
        query = query.filter(
            or_(
                Permit.source_record_id.ilike(f"%{qq}%"),
                Permit.address_clean.ilike(f"%{qq.upper()}%"),
                Permit.address_raw.ilike(f"%{qq}%"),
            )
        )

    return query.order_by(Permit.last_seen_at.desc()).limit(limit).all()

@app.get("/permits/{permit_id}", response_model=PermitOut)
def get_permit(permit_id: int, db: Session = Depends(get_db)):
    p = db.query(Permit).filter(Permit.id == permit_id).one_or_none()
    if not p:
        # fastapi default simple error
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")
    return p
@app.post("/ingest/capecoral")
def ingest_capecoral_now(db: Session = Depends(get_db)):
    return ingest_capecoral(db, days_back=settings.ingest_days_back)
