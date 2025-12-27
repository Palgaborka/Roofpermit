from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# Stored in DATA_DIR so it persists in Docker / deployments
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "jurisdictions.db.json"


@dataclass
class Jurisdiction:
    id: int
    state: str
    name: str
    system: str
    portal_url: str
    active: int = 1


def _load_db() -> dict:
    if not DB_PATH.exists():
        return {"next_id": 1, "items": []}
    return json.loads(DB_PATH.read_text(encoding="utf-8"))


def _save_db(db: dict) -> None:
    DB_PATH.write_text(json.dumps(db, indent=2, sort_keys=True), encoding="utf-8")


def seed_default() -> None:
    """
    Seeds a few default jurisdictions ONCE.
    Safe to call repeatedly.
    """
    db = _load_db()
    items = db.get("items", [])

    def exists(state: str, system: str, portal_url: str) -> bool:
        s = state.strip().upper()
        sys = system.strip().lower()
        url = portal_url.strip()
        return any(
            (it.get("state") == s and it.get("system") == sys and it.get("portal_url") == url)
            for it in items
        )

    defaults = [
        # You can add/adjust these as you like
        {
            "state": "FL",
            "name": "WEST PALM BEACH",
            "system": "energov",
            "portal_url": "https://westpalmbeachfl-energovpub.tylerhost.net/apps/selfservice/WestPalmBeachFLProd#/search?m=2&ps=10&pn=1&em=true",
            "active": 1,
        },
        # Cape Coral example (adjust if you have a better canonical URL)
        {
            "state": "FL",
            "name": "CAPE CORAL",
            "system": "energov",
            "portal_url": "https://energovweb.capecoral.gov/EnerGovProd/selfservice#/search?m=2&ps=10&pn=1&em=true",
            "active": 1,
        },
    ]

    changed = False
    for d in defaults:
        if not exists(d["state"], d["system"], d["portal_url"]):
            new_id = int(db.get("next_id", 1))
            db["next_id"] = new_id + 1
            items.append(
                {
                    "id": new_id,
                    "state": d["state"].strip().upper(),
                    "name": d["name"].strip(),
                    "system": d["system"].strip().lower(),
                    "portal_url": d["portal_url"].strip(),
                    "active": int(d.get("active", 1)),
                }
            )
            changed = True

    if changed:
        db["items"] = items
        _save_db(db)


def list_active(state: str) -> List[Jurisdiction]:
    db = _load_db()
    s = (state or "").strip().upper()
    out: List[Jurisdiction] = []
    for it in db.get("items", []):
        if s and it.get("state") != s:
            continue
        if int(it.get("active", 0)) != 1:
            continue
        out.append(
            Jurisdiction(
                id=int(it["id"]),
                state=it.get("state", ""),
                name=it.get("name", ""),
                system=it.get("system", ""),
                portal_url=it.get("portal_url", ""),
                active=int(it.get("active", 1)),
            )
        )
    # stable ordering
    out.sort(key=lambda j: (j.state, j.name, j.id))
    return out


def get_by_id(jurisdiction_id: int) -> Optional[Jurisdiction]:
    db = _load_db()
    jid = int(jurisdiction_id)
    for it in db.get("items", []):
        if int(it.get("id", 0)) == jid:
            return Jurisdiction(
                id=int(it["id"]),
                state=it.get("state", ""),
                name=it.get("name", ""),
                system=it.get("system", ""),
                portal_url=it.get("portal_url", ""),
                active=int(it.get("active", 1)),
            )
    return None


def add_jurisdiction(state: str, name: str, system: str, portal_url: str, active: int = 1) -> int:
    """
    Adds a jurisdiction and returns numeric ID.
    Dedupes by (STATE, system, portal_url). If duplicate exists, returns existing ID.
    """
    db = _load_db()
    items = db.get("items", [])

    s = (state or "").strip().upper()
    n = (name or "").strip()
    sys = (system or "").strip().lower()
    url = (portal_url or "").strip()
    a = 1 if int(active) == 1 else 0

    if not s or not n or not sys or not url:
        raise ValueError("Missing state/name/system/portal_url")

    # Deduplicate
    for it in items:
        if it.get("state") == s and it.get("system") == sys and it.get("portal_url") == url:
            # Optionally update name/active if you want
            it["name"] = n or it.get("name", "")
            it["active"] = a
            _save_db(db)
            return int(it["id"])

    new_id = int(db.get("next_id", 1))
    db["next_id"] = new_id + 1
    items.append(
        {
            "id": new_id,
            "state": s,
            "name": n,
            "system": sys,
            "portal_url": url,
            "active": a,
        }
    )
    db["items"] = items
    _save_db(db)
    return new_id
