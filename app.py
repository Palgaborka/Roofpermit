from __future__ import annotations

import csv
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from parcels import fetch_parcel_objects_in_polygon
from utils import LeadRow, clean_street_address
from jurisdictions import add_jurisdiction, get_by_id, list_active, seed_default
from connectors import get_connector

from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas


# -----------------------------
# Config / paths
# -----------------------------
SECRET_KEY = (os.environ.get("SECRET_KEY", "") or "").strip() or "CHANGE_ME"

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="RoofSpy (Florida-Ready)")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def require_key(request: Request):
    k = request.query_params.get("k") or request.headers.get("x-app-key") or ""
    if k != SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Seed jurisdictions on startup (safe to call multiple times)
seed_default()


# -----------------------------
# Scan state (in-memory)
# -----------------------------
scan_lock = threading.Lock()
scan_thread: Optional[threading.Thread] = None
scan_stop_flag = False

scan_status: Dict[str, Any] = {
    "running": False,
    "total": 0,
    "done": 0,
    "good": 0,
    "started_at": "",
    "finished_at": "",
    "message": "",
}

scan_rows: List[LeadRow] = []
_last_all_csv: Optional[Path] = None
_last_good_csv: Optional[Path] = None


# -----------------------------
# Output helpers
# -----------------------------
def write_csv(path: Path, rows: List[LeadRow]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "address",
                "jurisdiction",
                "owner",
                "mailing_address",
                "phone",
                "query_used",
                "permit_no",
                "type_line",
                "roof_date_used",
                "issued",
                "finalized",
                "applied",
                "roof_years",
                "is_20plus",
                "status",
                "seconds",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.address,
                    r.jurisdiction,
                    r.owner,
                    r.mailing_address,
                    r.phone,
                    r.query_used,
                    r.permit_no,
                    r.type_line,
                    r.roof_date_used,
                    r.issued,
                    r.finalized,
                    r.applied,
                    r.roof_years,
                    r.is_20plus,
                    r.status,
                    r.seconds,
                ]
            )


def rows_to_pdf_bytes(rows: List[LeadRow], title: str) -> bytes:
    from io import BytesIO

    buf = BytesIO()
    page_size = landscape(letter)
    c = canvas.Canvas(buf, pagesize=page_size)

    width, height = page_size
    left = 0.45 * inch
    top = height - 0.5 * inch
    line_h = 12

    c.setFont("Helvetica-Bold", 16)
    c.drawString(left, top, title)
    c.setFont("Helvetica", 10)
    c.drawString(left, top - 18, time.strftime("%Y-%m-%d %H:%M:%S"))
    c.drawString(left + 340, top - 18, f"rows: {len(rows)}")

    y = top - 42

    headers = [
        "Address",
        "Owner",
        "Mailing",
        "Roof Date",
        "Years",
        "20+?",
        "Permit #",
        "Type",
        "Status",
    ]

    # Make it narrower (mobile friendly) and avoid phone column
    col_x = [
        left,            # Address
        left + 240,      # Owner
        left + 410,      # Mailing
        left + 640,      # Roof Date
        left + 720,      # Years
        left + 770,      # 20+?
        left + 820,      # Permit #
        left + 910,      # Type
        left + 1000,     # Status
    ]

    def clip(s: str, maxlen: int) -> str:
        s = (s or "")
        return s if len(s) <= maxlen else (s[: maxlen - 1] + "…")

    def draw_row(vals, bold=False):
        nonlocal y
        if y < 0.6 * inch:
            c.showPage()
            y = top
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 8.7)
        for i, v in enumerate(vals):
            c.drawString(col_x[i], y, v or "")
        y -= line_h

    draw_row(headers, bold=True)
    c.line(left, y + 3, width - left, y + 3)
    y -= 6

    for r in rows:
        draw_row(
            [
                clip(r.address, 36),
                clip(r.owner, 22),
                clip(r.mailing_address, 30),
                clip(r.roof_date_used, 12),
                clip(r.roof_years, 6),
                "YES" if r.is_20plus == "True" else ("NO" if r.is_20plus == "False" else ""),
                clip(r.permit_no, 12),
                clip(r.type_line, 16),
                clip(r.status, 18),
            ],
            bold=False,
        )

    c.save()
    return buf.getvalue()


# -----------------------------
# Scan runner
# -----------------------------
def run_scan(parcels: List[Dict[str, str]], jurisdiction_id: int, delay_seconds: float, fast_mode: bool):
    global scan_stop_flag, _last_all_csv, _last_good_csv

    j = get_by_id(jurisdiction_id)
    if not j or int(getattr(j, "active", 0)) != 1:
        with scan_lock:
            scan_status.update({"running": False, "message": "Invalid jurisdiction."})
        return

    connector = get_connector(j)
    jname = j.name

    # Map address -> contact fields
    contact_by_addr: Dict[str, Dict[str, str]] = {}
    addresses: List[str] = []

    for p in parcels:
        addr = clean_street_address((p.get("address") or "").strip())
        if not addr:
            continue
        addresses.append(addr)
        contact_by_addr[addr] = {
            "owner": (p.get("owner") or "").strip(),
            "mailing_address": (p.get("mailing_address") or "").strip(),
            "phone": (p.get("phone") or "").strip(),
        }

    with scan_lock:
        scan_status.update(
            {
                "running": True,
                "total": len(addresses),
                "done": 0,
                "good": 0,
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "finished_at": "",
                "message": f"Scanning {jname}…",
            }
        )
        scan_rows.clear()
        scan_stop_flag = False

    consecutive_errors = 0
    base_delay = max(0.6, float(delay_seconds))
    if fast_mode:
        base_delay = max(0.4, float(delay_seconds))

    try:
        for addr in addresses:
            with scan_lock:
                if scan_stop_flag:
                    scan_status["message"] = "Stopped."
                    break

            t0 = time.time()
            cinfo = contact_by_addr.get(addr, {})
            owner = cinfo.get("owner", "")
            mailing = cinfo.get("mailing_address", "")
            phone = cinfo.get("phone", "")

            try:
                res = connector.search_roof(addr)
                elapsed = time.time() - t0

                if not res.get("roof_detected"):
                    err = (res.get("error") or "").strip()
                    status = "NO_ROOF_PERMIT_FOUND" if not err else f"ERROR: {err}"
                    row = LeadRow(
                        address=addr,
                        jurisdiction=jname,
                        owner=owner,
                        mailing_address=mailing,
                        phone=phone,
                        query_used=str(res.get("query_used", "")),
                        status=status,
                        seconds=f"{elapsed:.1f}",
                    )
                    consecutive_errors = consecutive_errors + 1 if err else 0
                else:
                    yrs_val = res.get("roof_years", "")
                    yrs_str = ""
                    if yrs_val != "":
                        try:
                            yrs_str = f"{float(yrs_val):.1f}"
                        except Exception:
                            yrs_str = str(yrs_val)

                    row = LeadRow(
                        address=addr,
                        jurisdiction=jname,
                        owner=owner,
                        mailing_address=mailing,
                        phone=phone,
                        query_used=str(res.get("query_used", "")),
                        permit_no=str(res.get("permit_no", "")),
                        type_line=str(res.get("type_line", "")),
                        roof_date_used=str(res.get("roof_date", "")),
                        issued=str(res.get("issued", "")),
                        finalized=str(res.get("finalized", "")),
                        applied=str(res.get("applied", "")),
                        roof_years=yrs_str,
                        is_20plus=str(res.get("is_20plus", "")),
                        status="OK",
                        seconds=f"{elapsed:.1f}",
                    )
                    consecutive_errors = 0

                with scan_lock:
                    scan_rows.append(row)
                    scan_status["done"] += 1
                    if row.is_20plus == "True":
                        scan_status["good"] += 1
                    scan_status["message"] = f"Last: {addr} ({row.seconds}s)"

            except Exception as e:
                elapsed = time.time() - t0
                consecutive_errors += 1
                row = LeadRow(
                    address=addr,
                    jurisdiction=jname,
                    owner=owner,
                    mailing_address=mailing,
                    phone=phone,
                    status=f"ERROR: {type(e).__name__}: {e}",
                    seconds=f"{elapsed:.1f}",
                )
                with scan_lock:
                    scan_rows.append(row)
                    scan_status["done"] += 1
                    scan_status["message"] = f"Last ERROR: {addr} ({row.seconds}s)"

            extra = 0.0
            if consecutive_errors >= 4:
                extra = 1.6
            elif consecutive_errors == 3:
                extra = 1.0
            elif consecutive_errors == 2:
                extra = 0.6

            jitter = random.uniform(0.15, 0.55)
            time.sleep(base_delay + extra + jitter)

    finally:
        ts = time.strftime("%Y%m%d_%H%M%S")
        all_path = DATA_DIR / f"leads_all_{ts}.csv"
        good_path = DATA_DIR / f"leads_good_20plus_{ts}.csv"

        with scan_lock:
            rows_copy = list(scan_rows)
            good_copy = [r for r in rows_copy if r.is_20plus == "True"]
            scan_status["running"] = False
            scan_status["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            scan_status["message"] = "Done."

        write_csv(all_path, rows_copy)
        write_csv(good_path, good_copy)
        _last_all_csv = all_path
        _last_good_csv = good_path


# -----------------------------
# Routes
# -----------------------------
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    require_key(request)
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


@app.get("/api/jurisdictions")
def api_jurisdictions(request: Request):
    require_key(request)
    items = list_active("FL")
    return JSONResponse({"ok": True, "jurisdictions": [j.__dict__ for j in items]})


@app.post("/api/jurisdictions/add")
async def api_jurisdictions_add(request: Request):
    require_key(request)
    body = await request.json()

    state = (body.get("state") or "FL").strip().upper()
    name = (body.get("name") or "").strip()
    system = (body.get("system") or "").strip().lower()
    portal_url = (body.get("portal_url") or "").strip()

    if not name or not system or not portal_url:
        return JSONResponse({"ok": False, "error": "Missing name/system/portal_url"})

    new_id = add_jurisdiction(state, name, system, portal_url, active=1)
    return JSONResponse({"ok": True, "id": new_id})


@app.get("/api/status")
def api_status(request: Request):
    require_key(request)
    with scan_lock:
        return JSONResponse(dict(scan_status))


@app.post("/api/parcels")
async def api_parcels(request: Request):
    require_key(request)
    body = await request.json()
    latlngs = body.get("latlngs")
    limit = int(body.get("limit", 80))

    if not latlngs or not isinstance(latlngs, list):
        return JSONResponse({"ok": False, "error": "Missing latlngs (draw an area first)."})
    try:
        parcels = fetch_parcel_objects_in_polygon(latlngs, limit=limit)
        return JSONResponse({"ok": True, "parcels": parcels})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.post("/api/start")
async def api_start(request: Request):
    global scan_thread
    require_key(request)
    body = await request.json()

    jurisdiction_id = int(body.get("jurisdiction_id") or 0)
    parcels = body.get("parcels") or []
    delay = float(body.get("delay", 1.0))
    fast_mode = bool(body.get("fast_mode", False))

    if not jurisdiction_id:
        return JSONResponse({"ok": False, "error": "Missing jurisdiction_id"})
    if not parcels or not isinstance(parcels, list):
        return JSONResponse({"ok": False, "error": "No parcels provided."})

    with scan_lock:
        if scan_status.get("running"):
            return JSONResponse({"ok": False, "error": "Scan already running."})

    def runner():
        run_scan(parcels, jurisdiction_id=jurisdiction_id, delay_seconds=delay, fast_mode=fast_mode)

    scan_thread = threading.Thread(target=runner, daemon=True)
    scan_thread.start()
    return JSONResponse({"ok": True})


@app.post("/api/stop")
def api_stop(request: Request):
    global scan_stop_flag
    require_key(request)
    with scan_lock:
        scan_stop_flag = True
        scan_status["message"] = "Stopping…"
    return JSONResponse({"ok": True})


@app.get("/download/all.pdf")
def download_all_pdf(request: Request):
    require_key(request)
    with scan_lock:
        rows_copy = list(scan_rows)
    if not rows_copy:
        raise HTTPException(404, "No results in memory yet. Run a scan first.")
    pdf = rows_to_pdf_bytes(rows_copy, title="RoofSpy Results (ALL)")
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=roofspy_all.pdf"},
    )


@app.get("/download/good.pdf")
def download_good_pdf(request: Request):
    require_key(request)
    with scan_lock:
        rows_copy = [r for r in scan_rows if r.is_20plus == "True"]
    if not rows_copy:
        raise HTTPException(404, "No 20+ year leads in memory yet. Run a scan first.")
    pdf = rows_to_pdf_bytes(rows_copy, title="RoofSpy Results (GOOD 20+)")
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": "inline; filename=roofspy_good_20plus.pdf"},
    )


@app.get("/download/all")
def download_all(request: Request):
    require_key(request)
    if not _last_all_csv or not _last_all_csv.exists():
        raise HTTPException(404, "No CSV available yet.")
    return FileResponse(str(_last_all_csv), filename=_last_all_csv.name)


@app.get("/download/good")
def download_good(request: Request):
    require_key(request)
    if not _last_good_csv or not _last_good_csv.exists():
        raise HTTPException(404, "No CSV available yet.")
    return FileResponse(str(_last_good_csv), filename=_last_good_csv.name)


# ============================================================
# DEBUG ROUTES (EnerGov DOM debugging)
# ============================================================
@app.get("/debug/energov.png")
def debug_energov_png(request: Request):
    require_key(request)
    p = DATA_DIR / "energov_debug.png"
    if not p.exists():
        raise HTTPException(404, "No debug screenshot yet. Run a scan that triggers debug output first.")
    return FileResponse(str(p), filename="energov_debug.png")


@app.get("/debug/energov.html")
def debug_energov_html(request: Request):
    require_key(request)
    p = DATA_DIR / "energov_debug.html"
    if not p.exists():
        raise HTTPException(404, "No debug html yet. Run a scan that triggers debug output first.")
    return FileResponse(str(p), filename="energov_debug.html")
