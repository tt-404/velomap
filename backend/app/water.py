"""Wasser-Ingest: BAFU Hydrodaten via api.existenz.ch → SQLite."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .db import WaterReading, WaterStation, get_session, init_db

log = logging.getLogger("water")

_BASE = "https://api.existenz.ch/apiv1/hydro"
_TECDOTTIR_BASE = "https://tecdottir.metaodi.ch/measurements"

# BAFU-Stationen (Kanton Zürich via existenz.ch)
STATION_IDS = [
    "2099", "2176", "2209", "2082", "2081", "2044", "2132", "2415", "2392",
    "2014", "2125", "2126", "2288", "520",
]
_LOC_PARAM = ",".join(STATION_IDS)

# Stadt-Zürich-Stationen (Wasserschutzpolizei via tecdottir.metaodi.ch)
_TECDOTTIR_STATIONS = {
    "zh-mythenquai":    {"name": "Zürichsee Mythenquai",    "water_body": "Zürichsee", "water_type": "lake", "lat": 47.3596, "lon": 8.5386, "slug": "mythenquai"},
    "zh-tiefenbrunnen": {"name": "Zürichsee Tiefenbrunnen", "water_body": "Zürichsee", "water_type": "lake", "lat": 47.3482, "lon": 8.5597, "slug": "tiefenbrunnen"},
}


def _fetch_locations() -> dict:
    """Stationsliste (Name, Gewässer, Koordinaten) von existenz.ch."""
    with httpx.Client(timeout=30.0) as c:
        r = c.get(f"{_BASE}/locations")
        r.raise_for_status()
    return r.json().get("payload", {})


def _ensure_stations() -> None:
    locations = _fetch_locations()
    with get_session() as s:
        for sid in STATION_IDS:
            info = locations.get(sid, {}).get("details", {})
            s.merge(WaterStation(
                id=sid,
                name=info.get("name", f"Station {sid}"),
                water_body=info.get("water-body-name"),
                water_type=info.get("water-body-type"),
                lat=info.get("lat"),
                lon=info.get("lon"),
            ))


def _payload_to_records(payload: list[dict]) -> list[dict]:
    """Wandelt flache {timestamp, loc, par, val}-Liste in {station_id, ts, temp, height}-Dicts."""
    # Erst nach (loc, timestamp) gruppieren
    grouped: dict[tuple, dict] = {}
    for item in payload:
        key = (str(item["loc"]), int(item["timestamp"]))
        if key not in grouped:
            grouped[key] = {"station_id": str(item["loc"]),
                            "ts": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc),
                            "temperature": None, "height": None}
        par = item.get("par")
        val = item.get("val")
        if par == "temperature":
            grouped[key]["temperature"] = float(val) if val is not None else None
        elif par == "height":
            grouped[key]["height"] = float(val) if val is not None else None
    return list(grouped.values())


def _ingest_tecdottir(initial: bool = False) -> int:
    """Holt Messungen von Mythenquai + Tiefenbrunnen (Stadt Zürich)."""
    # Stationen sicherstellen
    with get_session() as s:
        for sid, info in _TECDOTTIR_STATIONS.items():
            s.merge(WaterStation(id=sid, name=info["name"], water_body=info["water_body"],
                                 water_type=info["water_type"], lat=info["lat"], lon=info["lon"]))

    records = []
    if initial:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=8)
        extra = {"startDate": start.strftime("%Y-%m-%d"), "endDate": end.strftime("%Y-%m-%d")}
        offsets = [0, 500]  # zwei Seiten à 500 = ~8 Tage
    else:
        extra = {}
        offsets = [0]

    for sid, info in _TECDOTTIR_STATIONS.items():
        for offset in offsets:
            params = {"sort": "timestamp_utc desc", **extra}
            if offset:
                params["offset"] = offset
            try:
                with httpx.Client(timeout=60.0) as c:
                    r = c.get(f"{_TECDOTTIR_BASE}/{info['slug']}", params=params)
                    r.raise_for_status()
                rows = r.json().get("result", [])
                if not rows:
                    break
                for row in rows:
                    vals = row.get("values", {})
                    temp = vals.get("water_temperature", {}).get("value")
                    level = vals.get("water_level", {}).get("value")
                    records.append({
                        "station_id": sid,
                        "ts": datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")),
                        "temperature": float(temp) if temp is not None else None,
                        "height": float(level) if level is not None else None,
                    })
            except Exception as e:
                log.warning("Tecdottir %s offset=%s fehlgeschlagen: %s", sid, offset, e)
                break
    return _bulk_insert(records)


def run_water_ingest(initial: bool = False) -> dict:
    """Holt Temperatur + Wasserstand für alle ZH-Stationen und schreibt in DB."""
    init_db()
    _ensure_stations()

    if initial:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=8)
        url = (f"{_BASE}/daterange?locations={_LOC_PARAM}"
               f"&parameters=temperature,height"
               f"&startDate={start.strftime('%Y-%m-%d')}"
               f"&endDate={end.strftime('%Y-%m-%d')}")
    else:
        url = f"{_BASE}/latest?locations={_LOC_PARAM}&parameters=temperature,height"

    log.info("Lade Hydrodaten: %s", url)
    try:
        with httpx.Client(timeout=60.0) as c:
            r = c.get(url)
            r.raise_for_status()
        payload = r.json().get("payload", [])
    except Exception as e:
        log.error("Hydrodaten-Fetch fehlgeschlagen: %s", e)
        return {"status": "error", "message": str(e)}

    records = _payload_to_records(payload)
    inserted = _bulk_insert(records)
    inserted += _ingest_tecdottir(initial=initial)
    log.info("Wasser-Ingest: %d Readings eingefügt (inkl. Tecdottir)", inserted)
    return {"status": "ok", "inserted": inserted}


def _bulk_insert(records: list[dict]) -> int:
    if not records:
        return 0
    inserted = 0
    CHUNK = 1000
    with get_session() as s:
        for i in range(0, len(records), CHUNK):
            chunk = records[i:i + CHUNK]
            stmt = sqlite_insert(WaterReading).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=["station_id", "ts"])
            result = s.execute(stmt)
            inserted += result.rowcount or 0
    return inserted


def get_water_latest() -> list[dict]:
    """Neuester Messwert pro Station (kein Zeitfenster – für Marker-Label-Fallback)."""
    with get_session() as s:
        latest_subq = (
            select(WaterReading.station_id, func.max(WaterReading.ts).label("max_ts"))
            .group_by(WaterReading.station_id)
            .subquery()
        )
        rows = s.execute(
            select(WaterReading)
            .join(latest_subq,
                  (WaterReading.station_id == latest_subq.c.station_id) &
                  (WaterReading.ts == latest_subq.c.max_ts))
        ).scalars().all()
        return [
            {"station_id": r.station_id, "ts": r.ts.isoformat(),
             "temperature": r.temperature, "height": r.height}
            for r in rows
        ]


def get_water_stations() -> list[dict]:
    with get_session() as s:
        rows = s.execute(select(WaterStation)).scalars().all()
        return [
            {"id": r.id, "name": r.name, "water_body": r.water_body,
             "water_type": r.water_type, "lat": r.lat, "lon": r.lon}
            for r in rows
        ]


def get_water_snapshot(at: datetime, window_seconds: int = 1800) -> list[dict]:
    """Neuester Mess­wert pro Station innerhalb ±window/2 um `at`."""
    half = timedelta(seconds=window_seconds / 2)
    t_from, t_to = at - half, at + half
    with get_session() as s:
        # Neuesten Zeitstempel pro Station im Fenster
        latest_subq = (
            select(WaterReading.station_id, func.max(WaterReading.ts).label("max_ts"))
            .where(WaterReading.ts >= t_from, WaterReading.ts < t_to)
            .group_by(WaterReading.station_id)
            .subquery()
        )
        rows = s.execute(
            select(WaterReading)
            .join(latest_subq,
                  (WaterReading.station_id == latest_subq.c.station_id) &
                  (WaterReading.ts == latest_subq.c.max_ts))
        ).scalars().all()
        return [
            {"station_id": r.station_id, "ts": r.ts.isoformat(),
             "temperature": r.temperature, "height": r.height}
            for r in rows
        ]


def get_water_history(station_id: str, days: int = 7) -> list[dict]:
    """7-Tage-Verlauf einer Station für das Chart."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with get_session() as s:
        rows = s.execute(
            select(WaterReading)
            .where(WaterReading.station_id == station_id, WaterReading.ts >= since)
            .order_by(WaterReading.ts)
        ).scalars().all()
        return [
            {"ts": r.ts.isoformat(), "temperature": r.temperature, "height": r.height}
            for r in rows
        ]
