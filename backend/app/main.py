"""FastAPI-App: API + Auslieferung des Frontends."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dateutil.parser import isoparse
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select

from .db import Count, Precip, Station, get_session, init_db
from .ingest import run_ingest
from .radar import ccs4_bounds_wgs84, render_radar_png_at, run_radar_ingest
from .scheduler import shutdown_scheduler, start_scheduler

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")

FRONTEND_DIR = Path(__file__).parent.parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Wenn DB leer: initialen Import im Hintergrund
    with get_session() as s:
        n_stations = s.scalar(select(func.count(Station.id))) or 0
    if n_stations == 0:
        log.info("Datenbank leer – starte Initial-Import im Hintergrund")
        import threading
        threading.Thread(target=lambda: run_ingest(initial=True), daemon=True).start()
    start_scheduler()
    yield
    shutdown_scheduler()


app = FastAPI(title="Velo-Zürich Monitor", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    with get_session() as s:
        n_stations = s.scalar(select(func.count(Station.id))) or 0
        n_counts = s.scalar(select(func.count(Count.id))) or 0
        n_precip = s.scalar(select(func.count(Precip.id))) or 0
        latest_count = s.scalar(select(func.max(Count.ts)))
        latest_precip = s.scalar(select(func.max(Precip.ts)))
    return {
        "status": "ok",
        "stations": n_stations,
        "counts": n_counts,
        "precip_values": n_precip,
        "latest_count": latest_count.isoformat() if latest_count else None,
        "latest_precip": latest_precip.isoformat() if latest_precip else None,
    }


@app.get("/api/stations")
def list_stations(include_inactive: bool = Query(False)):
    """Liste aller Velozählstellen.

    Standardmässig nur Stationen, die mindestens einmal einen Velo-Wert
    > 0 geliefert haben (filtert Wetter-/Lärm-Stationen ohne Velo-Sensorik aus).
    Mit include_inactive=true werden alle Stationen zurückgegeben.
    """
    with get_session() as s:
        if include_inactive:
            rows = s.execute(select(Station)).scalars().all()
        else:
            # Nur Stationen, die jemals Velo-Verkehr gemessen haben.
            # SUM(velo_in + velo_out) > 0 wäre auch möglich, aber EXISTS ist schneller.
            active_ids_subq = (
                select(Count.station_id)
                .where((Count.velo_in > 0) | (Count.velo_out > 0))
                .distinct()
                .scalar_subquery()
            )
            rows = s.execute(
                select(Station).where(Station.id.in_(active_ids_subq))
            ).scalars().all()
        return [
            {
                "id": r.id,
                "name": r.name,
                "abkuerzung": r.abkuerzung,
                "bezeichnung": r.bezeichnung,
                "street_name": r.street_name or r.name,
                "lat": r.lat,
                "lon": r.lon,
            }
            for r in rows
        ]



@app.get("/api/counts")
def get_counts(
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
    station_id: Optional[int] = None,
):
    """Liefert Zählwerte im Zeitraum (optional gefiltert nach Station)."""
    try:
        t_from = isoparse(from_)
        t_to = isoparse(to)
    except Exception:
        raise HTTPException(400, "Ungültiges Datum (ISO 8601 erwartet)")

    with get_session() as s:
        q = (
            select(
                Count.station_id,
                Count.ts,
                func.sum(Count.velo_in).label("velo_in"),
                func.sum(Count.velo_out).label("velo_out"),
            )
            .where(Count.ts >= t_from, Count.ts < t_to)
            .group_by(Count.station_id, Count.ts)
            .order_by(Count.ts)
        )
        if station_id is not None:
            q = q.where(Count.station_id == station_id)
        rows = s.execute(q).all()
        return [
            {
                "station_id": r.station_id,
                "ts": r.ts.isoformat(),
                "velo_in": int(r.velo_in or 0),
                "velo_out": int(r.velo_out or 0),
                "total": int((r.velo_in or 0) + (r.velo_out or 0)),
            }
            for r in rows
        ]


@app.get("/api/snapshot")
def snapshot(
    at: str = Query(..., description="Zeitpunkt, ISO 8601"),
    window: int = Query(900, description="Fenster in Sekunden (default 15min)"),
):
    """Aggregierte Werte aller Stationen in einem Zeitfenster um `at`."""
    try:
        t_at = isoparse(at)
    except Exception:
        raise HTTPException(400, "Ungültiger Zeitpunkt")
    half = timedelta(seconds=window / 2)
    t_from, t_to = t_at - half, t_at + half

    with get_session() as s:
        q = (
            select(
                Count.station_id,
                func.sum(Count.velo_in).label("velo_in"),
                func.sum(Count.velo_out).label("velo_out"),
            )
            .where(Count.ts >= t_from, Count.ts < t_to)
            .group_by(Count.station_id)
        )
        rows = s.execute(q).all()
        return [
            {
                "station_id": r.station_id,
                "velo_in": int(r.velo_in or 0),
                "velo_out": int(r.velo_out or 0),
                "total": int((r.velo_in or 0) + (r.velo_out or 0)),
            }
            for r in rows
        ]


@app.get("/api/range")
def time_range():
    """Verfügbarer Zeitraum in der Datenbank."""
    with get_session() as s:
        t_min = s.scalar(select(func.min(Count.ts)))
        t_max = s.scalar(select(func.max(Count.ts)))
    return {
        "min": t_min.isoformat() if t_min else None,
        "max": t_max.isoformat() if t_max else None,
    }


@app.get("/api/precip")
def get_precip(
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
    station_id: Optional[int] = None,
):
    """Niederschlagswerte (mm/h) im Zeitraum, optional pro Station."""
    try:
        t_from = isoparse(from_)
        t_to = isoparse(to)
    except Exception:
        raise HTTPException(400, "Ungültiges Datum (ISO 8601 erwartet)")

    with get_session() as s:
        q = (
            select(Precip.station_id, Precip.ts, Precip.value, Precip.product)
            .where(Precip.ts >= t_from, Precip.ts < t_to)
            .order_by(Precip.ts)
        )
        if station_id is not None:
            q = q.where(Precip.station_id == station_id)
        rows = s.execute(q).all()
        return [
            {
                "station_id": r.station_id,
                "ts": r.ts.isoformat(),
                "value": r.value,
                "product": r.product,
            }
            for r in rows
        ]


@app.get("/api/precip/snapshot")
def precip_snapshot(at: str = Query(...), window: int = Query(1800)):
    """Niederschlag aller Stationen zu einem Zeitpunkt (Default ±30 Min Fenster)."""
    try:
        t_at = isoparse(at)
    except Exception:
        raise HTTPException(400, "Ungültiger Zeitpunkt")
    half = timedelta(seconds=window / 2)
    t_from, t_to = t_at - half, t_at + half

    with get_session() as s:
        # Pro Station den jüngsten Wert im Fenster
        q = (
            select(
                Precip.station_id,
                func.max(Precip.value).label("value"),
            )
            .where(Precip.ts >= t_from, Precip.ts < t_to)
            .group_by(Precip.station_id)
        )
        rows = s.execute(q).all()
        return [
            {"station_id": r.station_id, "value": float(r.value) if r.value else 0.0}
            for r in rows
        ]


@app.post("/api/ingest/trigger")
def trigger_ingest(background: BackgroundTasks, initial: bool = False):
    """Manueller Trigger für Velo-Ingest."""
    background.add_task(run_ingest, initial=initial)
    return {"status": "scheduled", "type": "velo", "initial": initial}


@app.post("/api/ingest/radar")
def trigger_radar(background: BackgroundTasks):
    """Manueller Trigger für Radar-Ingest."""
    background.add_task(run_radar_ingest)
    return {"status": "scheduled", "type": "radar"}


@app.get("/api/radar/bounds")
def radar_bounds():
    """WGS84-Grenzen des CCS4-Grids für Leaflet imageOverlay."""
    return ccs4_bounds_wgs84()


@app.get("/api/radar/image.png")
def radar_image_png(at: str = Query(None)):
    """RZC-Radar als RGBA-PNG zum angegebenen Zeitpunkt (MeteoSwiss OpenData)."""
    try:
        ts = isoparse(at) if at else datetime.now(timezone.utc)
    except Exception:
        raise HTTPException(400, "Ungültiger Zeitpunkt")
    png = render_radar_png_at(ts)
    if not png:
        raise HTTPException(503, "Kein Radar-Bild für diesen Zeitpunkt verfügbar")
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=300"},
    )


# === Frontend serven ===
if FRONTEND_DIR.exists():
    @app.get("/")
    def root():
        return FileResponse(FRONTEND_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
