from __future__ import annotations
import csv
import os
import threading
import time
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from parcels import fetch_parcel_addresses_in_polygon
from scanner import EnerGovScanner
from utils import LeadRow, clean_street_address

# PDF
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

# -----------------------------
# CONFIG / PRIVATE ACCESS
# -----------------------------

SECRET_KEY = os.environ.get("SECRET_KEY", "").strip()
if not SECRET_KEY:
    SECRET_KEY = "CHANGE_ME"

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATIC_DIR = Path(__file__).parent / "static"

# -----------------------------
# APP
# -----------------------------

app = FastAPI(title="RoofSpy (Private)")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

def require_key(request: Request):
    k = request.query_params.get("k") or request.headers.get("x-app-key") or ""
    if k != SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# -----------------------------
# Scan state
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

def write_csv(path: Path, rows: List[LeadRow]):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "address", "location", "query_used", "permit_no", "type_line",
            "roof_date_used", "issued", "finalized", "applied",
            "roof_years", "is_20plus", "status", "seconds"
        ])
        for r in rows:
            w.writerow([
                r.address, r.location, r.query_used, r.permit_no, r.type_line,
                r.roof_date_used, r.issued, r.finalized, r.applied,
                r.roof_years, r.is_20plus, r.status, r.seconds
            ])

def rows_to_pdf_bytes(rows: List[LeadRow], title: str) -> bytes:
    """
    Simple PDF report that opens directly in Safari.
    """
    from io import BytesIO
    buf = BytesIO()

    # Landscape letter for more columns
    page_size = landscape(letter)
    c = canvas.Canvas(buf, pagesize=page_size)

    width, height = page_size
    left = 0.5 * inch
    top = height - 0.5 * inch
    line_h = 12

    c.setFont("Helvetica-Bold", 16)
    c.drawString(left, top, title)
    c.setFont("Helvetica", 10)
    c.drawString(left, top - 18, time.strftime("%Y-%m-%d %H:%M:%S"))

    y = top - 40

    # Columns (keep it readable on phone)
    headers = ["Address", "Roof Date", "Years", "20+?", "Permit #", "Type", "Status"]
    col_x = [left, left + 230, left + 310, left + 360, left + 420, left + 520, left + 720]

    def draw_row(vals, bold=False):
        nonlocal y
        if y < 0.6 * inch:
            c.showPage()
            y = top
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 9)
        for i, v in enumerate(vals):
            txt = (v or "")
            # hard truncate to keep layout
            if len(txt) > 42 and i in (0, 5, 6):
                txt = txt[:39] + "…"
            if len(txt) > 26 and i in (4,):
                txt = txt[:23] + "…"
            c.drawString(col_x[i], y, txt)
        y -= line_h

    draw_row(headers, bold=True)
    c.line(left, y + 3, width - left, y + 3)
    y -= 6

    for r in rows:
        draw_row([
            r.address,
            r.roof_date_used,
            r.roof_years,
            "YES" if r.is_20plus == "True" else ("NO" if r.is_20plus else ""),
            r.permit_no,
            r.type_line,
            r.status
        ], bold=False)

    c.save()
    return buf.getvalue()

def run_scan(addresses: List[str], delay_seconds: float, fast_mode: bool):
    global scan_stop_flag, _last_all_csv, _last_good_csv

    with scan_lock:
        scan_status.update({
            "running": True,
            "total": len(addresses),
            "done": 0,
            "good": 0,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": "",
            "message": "Starting scanner…",
        })
        scan_rows.clear()
        scan_stop_flag = False

    consecutive_errors = 0
    base_delay = max(0.8, float(delay_seconds))
    if fast_mode:
        base_delay = max(0.5, float(delay_seconds))

    try:
        with EnerGovScanner(fast_mode=fast_mode) as scanner:
            with scan_lock:
                scan_status["message"] = f"Scanning… (fast_mode={'ON' if fast_mode else 'OFF'})"

            for addr in addresses:
                with scan_lock:
                    if scan_stop_flag:
                        scan_status["message"] = "Stopped."
                        break

                addr_clean = clean_street_address(addr)
                t0 = time.time()

                try:
                    res = scanner.search_address(addr_clean)
                    elapsed = time.time() - t0

                    if not res.get("roof_detected"):
                        err = (res.get("error") or "").strip()
                        status = "NO_ROOF_PERMIT_FOUND" if not err else f"ERROR: {err}"
                        row = LeadRow(
                            address=addr_clean,
                            query_used=str(res.get("query_used", "")),
                            status=status,
                            seconds=f"{elapsed:.1f}",
                        )
                        if err:
                            consecutive_errors += 1
                        else:
                            consecutive_errors = 0
                    else:
                        yrs_val = res.get("roof_years", "")
                        yrs_str = ""
                        if yrs_val != "":
                            try:
                                yrs_str = f"{float(yrs_val):.1f}"
                            except Exception:
                                yrs_str = str(yrs_val)

                        row = LeadRow(
                            address=addr_clean,
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
                        scan_status["message"] = f"Last: {addr_clean} ({row.seconds}s) | errors-in-row={consecutive_errors}"

                except Exception as e:
                    elapsed = time.time() - t0
                    consecutive_errors += 1
                    row = LeadRow(address=addr_clean, status=f"ERROR: {type(e).__name__}: {e}", seconds=f"{elapsed:.1f}")
                    with scan_lock:
                        scan_rows.append(row)
                        scan_status["done"] += 1
                        scan_status["message"] = f"Last ERROR: {addr_clean} ({row.seconds}s)"

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
            scan_status["message"] = f"Done. Saved: {all_path.name} and {good_path.name}"

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
        addresses = fetch_parcel_addresses_in_polygon(latlngs, limit=limit)
        return JSONResponse({"ok": True, "addresses": addresses})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.post("/api/start")
async def api_start(request: Request):
    global scan_thread
    require_key(request)
    body = await request.json()
    addresses = body.get("addresses") or []
    delay = float(body.get("delay", 1.0))
    fast_mode = bool(body.get("fast_mode", False))

    if not addresses:
        return JSONResponse({"ok": False, "error": "No addresses provided."})

    with scan_lock:
        if scan_status.get("running"):
            return JSONResponse({"ok": False, "error": "Scan already running."})

    def runner():
        run_scan(addresses, delay_seconds=delay, fast_mode=fast_mode)

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

# CSV downloads (still available)
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

# NEW: PDF opens directly in Safari
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
        headers={"Content-Disposition": "inline; filename=roofspy_all.pdf"}
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
        headers={"Content-Disposition": "inline; filename=roofspy_good_20plus.pdf"}
    )
