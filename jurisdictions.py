from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

from connectors.base import Jurisdiction

DB_PATH = Path("roofspy.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jurisdictions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  state TEXT NOT NULL,
  name TEXT NOT NULL,
  system TEXT NOT NULL,
  portal_url TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jurisdictions_state ON jurisdictions(state);
CREATE INDEX IF NOT EXISTS idx_jurisdictions_active ON jurisdictions(active);
"""

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

def seed_default():
    """
    Seed West Palm Beach as the first verified EnerGov jurisdiction.
    """
    init_db()
    conn = db()
    try:
        cur = conn.execute("SELECT COUNT(*) AS c FROM jurisdictions")
        c = cur.fetchone()["c"]
        if c > 0:
            return

        # WPB EnerGov portal you provided earlier
        wpb_url = "https://westpalmbeachfl-energovpub.tylerhost.net/apps/selfservice/WestPalmBeachFLProd#/search?m=2&ps=10&pn=1&em=true"

        conn.execute(
            "INSERT INTO jurisdictions(state,name,system,portal_url,active) VALUES (?,?,?,?,1)",
            ("FL", "City of West Palm Beach", "energov", wpb_url),
        )
        conn.commit()
    finally:
        conn.close()

def list_active(state: str = "FL") -> List[Jurisdiction]:
    init_db()
    conn = db()
    try:
        rows = conn.execute(
            "SELECT id,state,name,system,portal_url,active FROM jurisdictions WHERE state=? AND active=1 ORDER BY name",
            (state.upper(),),
        ).fetchall()
        return [Jurisdiction(**dict(r)) for r in rows]
    finally:
        conn.close()

def get_by_id(jurisdiction_id: int) -> Optional[Jurisdiction]:
    init_db()
    conn = db()
    try:
        r = conn.execute(
            "SELECT id,state,name,system,portal_url,active FROM jurisdictions WHERE id=?",
            (jurisdiction_id,),
        ).fetchone()
        return Jurisdiction(**dict(r)) if r else None
    finally:
        conn.close()

def add_jurisdiction(state: str, name: str, system: str, portal_url: str, active: int = 1) -> int:
    init_db()
    conn = db()
    try:
        cur = conn.execute(
            "INSERT INTO jurisdictions(state,name,system,portal_url,active) VALUES (?,?,?,?,?)",
            (state.upper(), name.strip(), system.strip().lower(), portal_url.strip(), int(active)),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()

def deactivate(jurisdiction_id: int):
    init_db()
    conn = db()
    try:
        conn.execute(
            "UPDATE jurisdictions SET active=0, updated_at=datetime('now') WHERE id=?",
            (jurisdiction_id,),
        )
        conn.commit()
    finally:
        conn.close()
