from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# Persisted storage
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
    DB_PATH.write_text(
        json.dumps(db, indent=2, sort_keys=True),
        encoding="utf-8"
    )


def seed_default() -> None:
    """
    Safe to call multiple times.
    Seeds known jurisdictions once.
    """
    db = _load_db()
    items = db.get("items", [])

    # ✅ Use FULL WPB URL (with params). This prevents EnerGov UI weirdness.
    defaults = [
        {
            "state": "FL",
            "name": "WEST PALM BEACH",
            "system": "energov",
            "portal_url": "https://westpalmbeachfl-energovpub.tylerhost.net/apps/selfservice/WestPalmBeachFLProd#/search?m=2&ps=10&pn=1&em=true",
            "active": 1,
        }
    ]

    for d in defaults:
        exists = any(
            it.get("state") == d["state"]
            and it.get("system") == d["system"]
            and it.get("portal_url") == d["portal_url"]
            for it in items
        )
        if not exists:
            new_id = int(db.get("next_id", 1))
            db["next_id"] = new_id + 1
            items.append(
                {
                    "id": new_id,
                    "state": d["state"],
                    "name": d["name"],
                    "system": d["system"],
                    "portal_url": d["portal_url"],
                    "active": int(d.get("active", 1)),
                }
            )

    db["items"] = items
    _save_db(db)


def list_active(state: str) -> List[Jurisdiction]:
    db = _load_db()
    s = (state or "").strip().upper()
    out: List[Jurisdiction] = []

    for it in db.get("items", []):
        if (it.get("state") or "").strip().upper() != s:
            continue
        if int(it.get("active", 0)) != 1:
            continue
        out.append(
            Jurisdiction(
                id=int(it["id"]),
                state=it["state"],
                name=it["name"],
                system=it["system"],
                portal_url=it["portal_url"],
                active=int(it.get("active", 1)),
            )
        )

    out.sort(key=lambda j: (j.name, j.id))
    return out


def get_by_id(jurisdiction_id: int) -> Optional[Jurisdiction]:
    db = _load_db()
    jid = int(jurisdiction_id)

    for it in db.get("items", []):
        if int(it.get("id", 0)) == jid:
            return Jurisdiction(
                id=int(it["id"]),
                state=it["state"],
                name=it["name"],
                system=it["system"],
                portal_url=it["portal_url"],
                active=int(it.get("active", 1)),
            )
    return None


def add_jurisdiction(
    state: str,
    name: str,
    system: str,
    portal_url: str,
    active: int = 1,
) -> int:
    """
    Adds a jurisdiction or returns existing ID if duplicate.
    Dedupes by (state, system, portal_url).
    """
    db = _load_db()
    s = (state or "").strip().upper()
    n = (name or "").strip()
    sys = (system or "").strip().lower()
    url = (portal_url or "").strip()

    if not s or not n or not sys or not url:
        raise ValueError("Missing jurisdiction fields")

    # If identical entry exists, update name/active and return id
    for it in db.get("items", []):
        if it.get("state") == s and it.get("system") == sys and it.get("portal_url") == url:
            it["name"] = n
            it["active"] = int(active)
            _save_db(db)
            return int(it["id"])

    new_id = int(db.get("next_id", 1))
    db["next_id"] = new_id + 1

    db.setdefault("items", []).append(
        {
            "id": new_id,
            "state": s,
            "name": n,
            "system": sys,
            "portal_url": url,
            "active": int(active),
        }
    )
    _save_db(db)
    return new_id


def delete_jurisdiction(jurisdiction_id: int) -> bool:
    """
    Deletes a jurisdiction by numeric id.
    Returns True if deleted, False if not found.
    """
    db = _load_db()
    jid = int(jurisdiction_id)
    items = db.get("items", [])

    new_items = [it for it in items if int(it.get("id", 0)) != jid]
    if len(new_items) == len(items):
        return False

    db["items"] = new_items
    _save_db(db)
    return True
