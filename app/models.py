from sqlalchemy import String, DateTime, Integer, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from .db import Base

class Permit(Base):
    __tablename__ = "permits"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source_city: Mapped[str] = mapped_column(String, index=True)
    source_portal: Mapped[str] = mapped_column(String, index=True)  # "ENERGOV"
    source_record_id: Mapped[str] = mapped_column(String, index=True)  # permit/application #

    detail_url: Mapped[str | None] = mapped_column(String, nullable=True)

    address_raw: Mapped[str | None] = mapped_column(String, nullable=True)
    address_clean: Mapped[str | None] = mapped_column(String, index=True, nullable=True)

    permit_type_raw: Mapped[str | None] = mapped_column(String, nullable=True)
    permit_type_normalized: Mapped[str | None] = mapped_column(String, index=True, nullable=True)

    status_raw: Mapped[str | None] = mapped_column(String, nullable=True)
    status_normalized: Mapped[str | None] = mapped_column(String, index=True, nullable=True)

    filed_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    issued_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    final_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    contractor_name: Mapped[str | None] = mapped_column(String, nullable=True)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("source_city", "source_portal", "source_record_id", name="uq_source_record"),
        Index("ix_perm_addr_city", "source_city", "address_clean"),
    )
