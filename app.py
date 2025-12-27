# app.py

import csv
import os
import re
import threading
import time
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from parcels import fetch_parcel_objects_in_polygon
from utils import LeadRow, clean_street_address
from jurisdictions import seed_default, list_active, get_by_id, add_jurisdiction
from connectors import get_connector

from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

# URL-onboarding helpers (detection + optional reachability)
from jurisdiction_onboard import (
    detect_system_from_url,
    validate_url_reachable,
)

SECRET_KEY = os.environ.get("SECRET_KEY", "").strip() or "CHANGE_ME"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="RoofSpy (Florida-Ready)")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def require_key(request: Request):
    k = request.query_params.get("k") or request.headers.get("x-app-key") or ""
    if k != SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Seed default jurisdictions on startup
seed_default()

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


def write_csv(path: Path, rows: List[LeadRow]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "address", "jurisdiction", "owner", "mailing_address", "phone",
            "query_used", "permit_no", "type_line",
            "roof_date_used", "issued", "finalized", "applied",
            "roof_years", "is_20plus", "status", "seconds"
        ])
        for r in rows:
            w.writerow([
                r.address, r.jurisdiction, r.owner, r.mailing_address, r.phone,
                r.query_used, r.permit_no, r.type_line,
                r.roof_date_used, r.issued, r.finalized, r.applied,
                r.roof_years, r.is_20plus, r.status, r.seconds
            ])


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

    y = top - 42

    headers = ["Address", "Owner", "Mailing", "Phone", "Roof Date", "Years", "20+?", "Permit #", "Type", "Status"]
    col_x = [
        left,            # Address
        left + 230,      # Owner
        left + 395,      # Mailing
        left + 595,      # Phone
        left + 690,      # Roof Date
        left + 755,      # Years
        left + 805,      # 20+?
        left + 850,      # Permit #
        left + 940,      # Type
        left + 1085,     # Status
    ]

    def clip(s: str, maxlen: int) -> str:
        s = (s or "")
        return s if len(s) <= maxlen else (s[:maxlen - 1] + "…")

    def draw_row(vals, bold=False):
        nonlocal y
        if y < 0.6 * inch:
            c.showPage()
            y = top
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 8.5)
        for i, v in enumerate(vals):
            c.drawString(col_x[i], y, v or "")
        y -= line_h

    # header
    draw_row(headers, bold=True)
    c.line(left, y + 3, width - left, y + 3)
    y -= 6

    for r in rows:
        draw_row([
            clip(r.address, 34),
            clip(r.owner, 22),
            clip(r.mailing_address, 30),
            clip(r.phone, 14),
            r.roof_date_used,
            r.roof_years,
            "YES" if r.is_20plus == "True" else ("NO" if r.is_20plus else ""),
            clip(r.permit_no, 12),
            clip(r.type_line, 18),
            clip(r.status, 16),
        ], bold=False)

    c.save()
    return buf.getvalue()


# ---------------------------
# Jurisdiction Onboarding UI
# ---------------------------

@app.get("/jurisdictions/onboard", response_class=HTMLResponse)
def onboard_form(request: Request):
    require_key(request)
    return """
    <html>
      <head>
        <title>RoofSpy - Add Jurisdiction</title>
        <style>
          body{font-family:Arial, sans-serif; max-width:900px; margin:40px auto; padding:0 16px;}
          input, button{font-size:16px; padding:10px; width:100%;}
          .row{margin:14px 0;}
          .small{color:#666; font-size:13px;}
          code{background:#f2f2f2; padding:2px 6px; border-radius:6px;}
        </style>
      </head>
      <body>
        <h2>Add a City / Jurisdiction</h2>
        <p class="small">Paste a permit search URL. We auto-detect <b>EnerGov</b> vs <b>ArcGIS</b> and save it to your jurisdiction list.</p>

        <form method="post" action="/jurisdictions/onboard">
          <div class="row">
            <label>Permit search URL</label><br/>
            <input name="url" placeholder="https://...#/search?m=2&ps=10&pn=1&em=true" />
          </div>

          <div class="row">
            <label>State (default FL)</label><br/>
            <input name="state" value="FL" />
          </div>

          <div class="row">
            <label>Optional City Display Name (leave blank to auto)</label><br/>
            <input name="display_name" placeholder="CAPE CORAL" />
          </div>

          <div class="row">
            <button type="submit">Detect & Save</button>
          </div>
        </form>

        <p class="small">
          Tip: after saving, refresh your main app page; the city should appear in the jurisdiction dropdown.
        </p>
      </body>
    </html>
    """


@app.post("/jurisdictions/onboard", response_class=HTMLResponse)
def onboard_submit(
    request: Request,
    url: str = Form(...),
    state: str = Form("FL"),
    display_name: str = Form(""),
):
    require_key(request)

    try:
        detected = detect_system_from_url(url)

        # allow overrides
        detected.state = (state or detected.state or "FL").strip().upper()
        if display_name.strip():
            detected.display_name = display_name.strip()

        # lightweight reachability check (does not guarantee API works, just that URL responds)
        validation = validate_url_reachable(url)

        # IMPORTANT: Save into your existing jurisdictions DB/registry so IDs stay numeric
        # This keeps /api/start working (it expects int jurisdiction_id)
        new_id = add_jurisdiction(
            detected.state,
            detected.display_name,
            detected.system,
            detected.base_url,
            active=1
        )

        return f"""
        <html><body style="font-family:Arial; max-width:900px; margin:40px auto; padding:0 16px;">
          <h2>Saved ✅</h2>
          <p><b>ID:</b> {new_id}</p>
          <p><b>Name:</b> {detected.display_name}</p>
          <p><b>State:</b> {detected.state}</p>
          <p><b>System:</b> {detected.system}</p>
          <p><b>Base URL:</b> <a href="{detected.base_url}">{detected.base_url}</a></p>
          <p><b>Reachable check:</b> {validation}</p>

          <p><a href="/jurisdictions/onboard">Add another</a></p>
          <p><a href="/?k={SECRET_KEY}">Go to app home</a></p>
        </body></html>
        """
    except Exception as e:
        return f"""
        <html><body style="font-family:Arial; max-width:900px; margin:40px auto; padding:0 16px;">
          <h2>Error</h2>
          <p>{str(e)}</p>
          <p><a href="/jurisdictions/onboard">Go back</a></p>
        </body></html>
        """


# ---------------------------
# Scanning logic
# ---------------------------

def run_scan(parcels: List[Dict[str, str]], jurisdiction_id: int, delay_seconds: float, fast_mode: bool):
    global scan_stop_flag, _last_all_csv, _last_good_csv

    j = get_by_id(jurisdiction_id)
    if not j or j.active != 1:
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
        scan_status.update({
            "running": True,
            "total": len(addresses),
            "done": 0,
            "good": 0,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": "",
            "message": f"Scanning {jname}…",
        })
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


# ---------------------------
# Routes / API
# ---------------------------

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
