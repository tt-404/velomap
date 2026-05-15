"""Parkhaus-Ingest: PLS Zürich RSS-Feed → SQLite."""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import httpx
from sqlalchemy import select

from .db import ParkingGarage, ParkingReading, get_session

log = logging.getLogger("parking")

_RSS_URL = "https://www.pls-zh.ch/plsFeed/rss"
_NOMINATIM_UA = "velo-zh-monitor/1.0 (github.com/tt-404/velomap)"
_NS = {"dc": "http://purl.org/dc/elements/1.1/"}


def _slug_from_url(url: str) -> str:
    """Extrahiert pid aus PLS-URL, z.B. 'accu' aus '...accu.jsp?pid=accu'."""
    m = re.search(r"pid=([^&]+)", url)
    return m.group(1) if m else url.split("/")[-1].split(".")[0]


def _geocode(address: str) -> tuple[float, float] | tuple[None, None]:
    query = f"{address}, Zürich, Switzerland"
    try:
        with httpx.Client(timeout=10.0) as c:
            r = c.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1},
                headers={"User-Agent": _NOMINATIM_UA},
            )
            r.raise_for_status()
            results = r.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        log.warning("Geocode fehlgeschlagen für '%s': %s", address, e)
    return None, None


def run_parking_ingest() -> dict:
    """Holt PLS-RSS, parst Parkhäuser, schreibt in DB."""
    try:
        with httpx.Client(timeout=15.0) as c:
            resp = c.get(_RSS_URL, follow_redirects=True)
            resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        log.error("PLS-Feed nicht abrufbar: %s", e)
        return {"status": "error", "message": str(e)}

    items = root.findall(".//item")
    inserted = 0

    with get_session() as s:
        existing_slugs = set(
            r[0] for r in s.execute(select(ParkingGarage.slug)).all()
        )

    new_garages: list[ParkingGarage] = []

    for item in items:
        link = (item.findtext("link") or "").strip()
        slug = _slug_from_url(link)
        title = (item.findtext("title") or "").strip()
        desc = (item.findtext("description") or "").strip()
        dc_date = (item.findtext("dc:date", namespaces=_NS) or "").strip()

        # "open / 178" → status, free
        parts = [p.strip() for p in desc.split("/")]
        status = parts[0] if parts else "unknown"
        try:
            free = int(parts[1]) if len(parts) > 1 else None
        except ValueError:
            free = None

        # Zeitstempel
        try:
            ts = datetime.fromisoformat(dc_date.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)

        # Name und Adresse aus Titel ("Parkhaus Xyz / Strassenname 12")
        title_parts = [p.strip() for p in title.split(" / ", 1)]
        name = title_parts[0]
        address = title_parts[1] if len(title_parts) > 1 else name

        # Neues Parkhaus geocodieren (1 req/s Limit beachten)
        if slug not in existing_slugs:
            lat, lon = _geocode(address)
            time.sleep(1.1)
            garage = ParkingGarage(slug=slug, name=name, address=address, lat=lat, lon=lon)
            new_garages.append(garage)
            existing_slugs.add(slug)

        # Reading einfügen
        with get_session() as s:
            if new_garages:
                for g in new_garages:
                    s.merge(g)
                new_garages.clear()
            reading = ParkingReading(garage_slug=slug, ts=ts, free=free, status=status)
            try:
                s.add(reading)
                s.flush()
                inserted += 1
            except Exception:
                s.rollback()

    log.info("Parking-Ingest: %d Readings eingefügt", inserted)
    return {"status": "ok", "inserted": inserted}


def get_current_parking() -> list[dict]:
    """Liefert aktuellste Belegung aller Parkhäuser."""
    from sqlalchemy import func
    with get_session() as s:
        # Neuester Zeitstempel pro Garage
        latest_subq = (
            select(ParkingReading.garage_slug, func.max(ParkingReading.ts).label("max_ts"))
            .group_by(ParkingReading.garage_slug)
            .subquery()
        )
        rows = s.execute(
            select(ParkingGarage, ParkingReading)
            .join(ParkingReading, ParkingGarage.slug == ParkingReading.garage_slug)
            .join(latest_subq, (ParkingReading.garage_slug == latest_subq.c.garage_slug)
                  & (ParkingReading.ts == latest_subq.c.max_ts))
        ).all()
        return [
            {
                "slug": g.slug,
                "name": g.name,
                "address": g.address,
                "lat": g.lat,
                "lon": g.lon,
                "free": r.free,
                "status": r.status,
                "ts": r.ts.isoformat(),
            }
            for g, r in rows
        ]
