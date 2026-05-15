"""MIV-Ingest: Strassenverkehrszählung Stadt Zürich → SQLite."""
from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta

import httpx
import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .coords import lv95_to_wgs84
from .db import MivCount, MivStation, get_session, init_db

log = logging.getLogger("miv")

# Staatsstrassen (Kanton Zürich) — Achse-Muster, case-insensitive
_KANTONAL_PATTERNS = [
    "seestrasse", "albisstrasse", "birmensdorferstrasse", "badenerstrasse",
    "limmattalstrasse", "regensdorferstrasse", "wehntalerstrasse",
    "schaffhauserstrasse", "thurgauerstrasse", "ueberlandstrasse",
    "dübendorfstrasse", "witikonerstrasse", "forchstrasse", "rämistrasse",
    "winterthurerstrasse", "wallisellenstrasse", "albisriederstrasse",
    "bellerivestrasse",
]

# Nationalstrassen — Achse beginnt mit A + Zahl oder enthält "Autobahn"
import re as _re
_NATIONAL_RE = _re.compile(r"(^|\s)(A\d|autobahn)", _re.IGNORECASE)


def classify_road(zsname: str, achse: str) -> str:
    text = (zsname + " " + achse).lower()
    if _NATIONAL_RE.search(achse):
        return "national"
    for pat in _KANTONAL_PATTERNS:
        if pat in text:
            return "kantonal"
    return "kommunal"


_CSV_URL = (
    "https://data.stadt-zuerich.ch/dataset/"
    "sid_dav_verkehrszaehlung_miv_od2031/download/"
    "sid_dav_verkehrszaehlung_miv_OD2031_{year}.csv"
)


def _csv_url(year: int) -> str:
    return _CSV_URL.format(year=year)


def run_miv_ingest(initial: bool = False) -> dict:
    """Lädt MIV-CSV und schreibt Stationen + Zählwerte in die DB."""
    init_db()
    today = datetime.now()
    years = [today.year - 1, today.year] if initial else [today.year]
    since = None if initial else datetime.now() - timedelta(days=10)

    total_inserted = 0
    for year in years:
        url = _csv_url(year)
        log.info("Lade MIV-CSV %s", url)
        try:
            with httpx.Client(timeout=300.0, follow_redirects=True) as c:
                r = c.get(url)
                r.raise_for_status()
        except httpx.HTTPStatusError as e:
            log.warning("MIV Jahr %s: HTTP %s – überspringe", year, e.response.status_code)
            continue
        except Exception as e:
            log.error("MIV Jahr %s: %s", year, e)
            continue

        df = pd.read_csv(io.BytesIO(r.content), encoding="utf-8-sig")
        log.info("MIV CSV %s geladen: %d Zeilen", year, len(df))

        _upsert_miv_stations(df)
        n = _insert_miv_counts(df, since=since)
        total_inserted += n
        log.info("MIV Jahr %s: %d neue Zählwerte", year, n)

    return {"status": "ok", "inserted": total_inserted}


def _upsert_miv_stations(df: pd.DataFrame) -> None:
    first = df.drop_duplicates(subset=["ZSID"])
    with get_session() as s:
        for row in first.itertuples(index=False):
            zsid = str(row.ZSID)
            achse = str(row.Achse) if hasattr(row, "Achse") else ""
            zsname = str(row.ZSName)
            try:
                lat, lon = lv95_to_wgs84(float(row.EKoord), float(row.NKoord))
            except Exception:
                lat, lon = None, None
            s.merge(MivStation(
                id=zsid,
                name=zsname,
                street=achse or None,
                road_class=classify_road(zsname, achse),
                lat=lat,
                lon=lon,
            ))


def _insert_miv_counts(df: pd.DataFrame, since: datetime | None) -> int:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["MessungDatZeit"], errors="coerce")
    df = df.dropna(subset=["ts", "ZSID", "AnzFahrzeuge"])
    if since is not None:
        df = df[df["ts"] >= pd.Timestamp(since)]

    records = [
        {
            "station_id": str(row.ZSID),
            "direction": str(row.Richtung) if hasattr(row, "Richtung") else None,
            "ts": row.ts.to_pydatetime(),
            "count": int(row.AnzFahrzeuge) if pd.notna(row.AnzFahrzeuge) else None,
        }
        for row in df.itertuples(index=False)
    ]

    if not records:
        return 0

    inserted = 0
    CHUNK = 5000
    with get_session() as s:
        for i in range(0, len(records), CHUNK):
            chunk = records[i:i + CHUNK]
            stmt = sqlite_insert(MivCount).values(chunk)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["station_id", "direction", "ts"]
            )
            result = s.execute(stmt)
            inserted += result.rowcount or 0
    return inserted


def get_miv_stations() -> list[dict]:
    from sqlalchemy import func, select
    with get_session() as s:
        # Nur Stationen mit Daten in den letzten 60 Tagen
        cutoff = datetime.now() - timedelta(days=60)
        active = set(
            r[0] for r in s.execute(
                select(MivCount.station_id)
                .where(MivCount.ts >= cutoff)
                .distinct()
            ).all()
        )
        rows = s.execute(
            __import__("sqlalchemy").select(MivStation)
            .where(MivStation.id.in_(active))
        ).scalars().all()
        return [
            {"id": r.id, "name": r.name, "street": r.street,
             "road_class": r.road_class or "kommunal", "lat": r.lat, "lon": r.lon}
            for r in rows
        ]


def get_miv_snapshot(at: datetime, window_seconds: int = 3600) -> list[dict]:
    """Aggregierte MIV-Zählwerte aller Stationen um einen Zeitpunkt (±window/2)."""
    from sqlalchemy import func, select
    from datetime import timedelta as td
    half = td(seconds=window_seconds / 2)
    t_from, t_to = at - half, at + half
    with get_session() as s:
        rows = s.execute(
            select(
                MivCount.station_id,
                func.sum(MivCount.count).label("total"),
            )
            .where(MivCount.ts >= t_from, MivCount.ts < t_to)
            .group_by(MivCount.station_id)
        ).all()
        return [
            {"station_id": r.station_id, "total": int(r.total or 0)}
            for r in rows
        ]
