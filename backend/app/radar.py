"""Ingestion der MeteoSwiss Niederschlagsradar-Daten via STAC-API.

Quelle: https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-radar-precip
Lizenz: Open Data (CC BY 4.0), Quellenangabe "MeteoSchweiz" erforderlich.

Verfügbarkeit: rollendes 14-Tage-Fenster. Wir holen alle 10 Min die neueste
CombiPrecip-1h-Summe (CPC) und extrahieren den Wert am nächstgelegenen Pixel
zu jeder Velozählstelle. Damit baut sich lokal eine längere Historie auf.

Produkt-Übersicht:
- RZC: PRECIP, 5-Min-Takt, mm/h instantan
- CPC: CombiPrecip 1h-Summe, 5-Min-Takt, mm akkumuliert
- CPCH: CPC-Reanalyse (8 Tage Delay, qualitativ besser)

Wir nutzen CPC, weil 1h-Summen besser zur 15-Min-Velo-Aggregation passen
und CombiPrecip durch die Korrektur mit Bodenstationen genauer ist.
"""
from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timedelta, timezone

import h5py
import httpx
import numpy as np
from pyproj import Transformer
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .db import IngestLog, Precip, Station, get_session, init_db

log = logging.getLogger("radar")

STAC_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION = "ch.meteoschweiz.ogd-radar-precip"

# CombiPrecip 1h-Summe (akkumuliert) – beste Genauigkeit dank Bodenstation-Korrektur
PRODUCT_PREFIX = "CPC"
PRODUCT_ACCUM = "00060"  # 60-Min-Akkumulation

# Transformer: WGS84 → LV95 (Radar-Daten sind in LV95)
_WGS_TO_LV95 = Transformer.from_crs("EPSG:4326", "EPSG:2056", always_xy=True)


def stations_lv95() -> dict[int, tuple[float, float]]:
    """Stations-Koordinaten in LV95 (für Radar-Pixel-Lookup)."""
    out = {}
    with get_session() as s:
        for st in s.execute(select(Station)).scalars():
            if st.lat is None or st.lon is None:
                continue
            x, y = _WGS_TO_LV95.transform(st.lon, st.lat)
            out[st.id] = (x, y)
    return out


def latest_cpc_item_url() -> str | None:
    """Findet das aktuellste CPC-Item via STAC-API."""
    # STAC-Items sind nach Datum gruppiert; wir nehmen das aktuellste Item
    # (= heutiger Tag), dann das letzte Asset darin.
    url = f"{STAC_BASE}/collections/{COLLECTION}/items?limit=2"
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()

    features = data.get("features", [])
    if not features:
        log.warning("STAC: keine Items in Collection %s", COLLECTION)
        return None

    # Sortiere nach datetime absteigend (neueste zuerst)
    features.sort(key=lambda f: f.get("properties", {}).get("datetime", ""), reverse=True)

    # Suche das aktuellste CPC-Asset (60min-Akkumulation)
    pattern = re.compile(rf"^{PRODUCT_PREFIX}\d+\d+_{PRODUCT_ACCUM}\.")
    for feat in features:
        assets = feat.get("assets", {})
        # Asset-Keys alphabetisch absteigend = zeitlich neueste zuerst
        for key in sorted(assets.keys(), reverse=True):
            if pattern.match(key):
                return assets[key].get("href")
    log.warning("STAC: kein CPC-Asset gefunden")
    return None


def parse_filename_timestamp(href: str) -> datetime | None:
    """Extrahiert den Zeitstempel aus dem Dateinamen.

    Beispiel: CPC25318143000_00060.801.h5
    Format:   CPC + yy + jjj + HHMM + Q + _ + nnnnn
              yy=25, jjj=318 (Tag 318 von 2025), HHMM=1430, Q=0
    """
    name = href.rsplit("/", 1)[-1]
    m = re.match(r"^[A-Z]{3}(\d{2})(\d{3})(\d{2})(\d{2})\d", name)
    if not m:
        return None
    yy, jjj, hh, mm = m.groups()
    year = 2000 + int(yy)
    try:
        # Tag im Jahr (1-366) → Datum
        d = datetime(year, 1, 1, int(hh), int(mm), tzinfo=timezone.utc) + timedelta(days=int(jjj) - 1)
        return d
    except ValueError:
        return None


def download_radar_file(url: str) -> bytes:
    """Lädt eine HDF5-Datei vom STAC-Asset-URL."""
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.content


def extract_values_at_stations(
    h5_bytes: bytes, stations_xy: dict[int, tuple[float, float]]
) -> dict[int, float]:
    """Extrahiert den Niederschlagswert am Pixel der jeweiligen Station.

    MeteoSwiss-Radarprodukte sind im ODIM-HDF5-Format. Das Bild liegt
    typischerweise unter /dataset1/data1/data, die Geo-Referenzierung
    in /where (LL_lon, LL_lat, UR_lon, UR_lat) oder /dataset1/where (xscale, yscale).

    Für CCS4-Produkte (Schweizer Composite, 710x640 Pixel, 1km Auflösung)
    sind die LV95-Ecken fix:
      Origin (oben-links): (255'000, 480'000) -> in LV95 (2'255'000, 1'480'000)
      Pixel-Grösse: 1000m
    """
    out: dict[int, float] = {}
    with h5py.File(io.BytesIO(h5_bytes), "r") as f:
        # Daten-Array suchen (ODIM-Standard: /dataset1/data1/data)
        try:
            data = f["/dataset1/data1/data"][...]
        except KeyError:
            log.error("HDF5: /dataset1/data1/data nicht gefunden")
            return out

        # Skalierung (ODIM: gain, offset, nodata, undetect)
        what = f["/dataset1/data1/what"]
        gain = float(what.attrs.get("gain", 1.0))
        offset = float(what.attrs.get("offset", 0.0))
        nodata = float(what.attrs.get("nodata", 65535))
        undetect = float(what.attrs.get("undetect", 0))

        # CCS4-Grid (Standard für MeteoSwiss-Composite)
        # Ursprung oben-links, Y wächst nach unten
        rows, cols = data.shape
        if (rows, cols) == (640, 710):
            # CCS4 1-km-Grid
            x_origin = 2_255_000.0
            y_origin = 1_480_000.0
            pixel_size = 1000.0
        elif (rows, cols) == (1280, 1410):
            # CCS4r2 500-m-Grid
            x_origin = 2_255_000.0
            y_origin = 1_480_000.0
            pixel_size = 500.0
        else:
            # Versuche aus /where zu lesen
            try:
                where = f["/where"]
                ll_lon = float(where.attrs.get("LL_lon"))
                ll_lat = float(where.attrs.get("LL_lat"))
                ur_lon = float(where.attrs.get("UR_lon"))
                ur_lat = float(where.attrs.get("UR_lat"))
                # konvertiere zu LV95
                x0, y0 = _WGS_TO_LV95.transform(ll_lon, ur_lat)  # oben-links
                x1, y1 = _WGS_TO_LV95.transform(ur_lon, ll_lat)  # unten-rechts
                x_origin, y_origin = x0, y0
                pixel_size = (x1 - x0) / cols
            except Exception as e:
                log.error("HDF5: Geo-Referenz konnte nicht ermittelt werden (%s)", e)
                return out

        for sid, (x, y) in stations_xy.items():
            col = int(round((x - x_origin) / pixel_size))
            row = int(round((y_origin - y) / pixel_size))
            if not (0 <= row < rows and 0 <= col < cols):
                continue
            raw = float(data[row, col])
            if raw == nodata:
                continue
            if raw == undetect:
                out[sid] = 0.0
                continue
            out[sid] = raw * gain + offset
    return out


def bulk_insert_precip(values: dict[int, float], ts: datetime, product: str) -> int:
    """Speichert Niederschlagswerte für alle Stationen zum gegebenen Zeitpunkt."""
    if not values:
        return 0
    records = [
        {"station_id": sid, "ts": ts, "value": float(v), "product": product}
        for sid, v in values.items()
    ]
    with get_session() as s:
        stmt = sqlite_insert(Precip).values(records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["station_id", "ts", "product"]
        )
        result = s.execute(stmt)
        return result.rowcount or 0


def run_radar_ingest() -> dict:
    """Hauptfunktion: holt das aktuellste CPC-Bild und extrahiert pro Station."""
    init_db()
    result = {"source": "meteoswiss-radar", "status": "ok", "rows": 0, "msg": ""}

    stations = stations_lv95()
    if not stations:
        result["status"] = "skipped"
        result["msg"] = "Keine Stationen in DB – Velo-Ingest zuerst laufen lassen"
        return result

    href = latest_cpc_item_url()
    if not href:
        result["status"] = "error"
        result["msg"] = "Kein CPC-Asset gefunden"
        return result

    ts = parse_filename_timestamp(href)
    if not ts:
        result["status"] = "error"
        result["msg"] = f"Konnte Zeitstempel nicht parsen: {href}"
        return result

    log.info("Lade Radar-Datei %s (ts=%s)", href.rsplit("/", 1)[-1], ts)
    try:
        h5_bytes = download_radar_file(href)
    except Exception as e:
        result["status"] = "error"
        result["msg"] = f"Download fehlgeschlagen: {e}"
        return result

    try:
        values = extract_values_at_stations(h5_bytes, stations)
    except Exception as e:
        result["status"] = "error"
        result["msg"] = f"HDF5-Parsing fehlgeschlagen: {e}"
        return result

    inserted = bulk_insert_precip(values, ts, product="CPC")
    result["rows"] = inserted
    result["msg"] = f"{inserted} Niederschlagswerte für {ts.isoformat()}"

    with get_session() as s:
        s.add(IngestLog(
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            rows_inserted=inserted,
            source="meteoswiss-radar",
            status=result["status"],
            message=result["msg"],
        ))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    print(run_radar_ingest())
