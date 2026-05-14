"""Ingestion der Velozähldaten der Stadt Zürich.

Quellen:
  - Standorte:  GeoJSON in WGS84 vom Geodatenportal
  - Zählwerte:  Jahres-CSVs (ein File pro Jahr) im 15-Min-Takt

Hinweis: Das Open-Data-CSV wird vom Tiefbauamt einmal täglich aktualisiert.
Der CSV-Endpunkt unterstützt kein Conditional-GET fein granular, daher
deduplizieren wir auf SQL-Ebene (UNIQUE-Index auf zaehler+station_id+ts).
"""
from __future__ import annotations

import argparse
import io
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx
import pandas as pd
from dateutil.parser import isoparse
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .coords import lv95_to_wgs84
from .db import Count, IngestLog, Station, get_session, init_db

log = logging.getLogger("ingest")

# === URLs der offiziellen Stadt-Zürich-Datensätze ===
STATIONS_GEOJSON_URL = (
    "https://www.ogd.stadt-zuerich.ch/wfs/geoportal/"
    "Standorte_der_automatischen_Fuss__und_Velozaehlungen?"
    "service=WFS&version=1.1.0&request=GetFeature&"
    "typename=ms:Standorte_der_automatischen_Fuss__und_Velozaehlungen&"
    "outputFormat=geojson"
)

# CSV-Dateien pro Jahr (Dateinamen-Schema ist stabil seit Jahren)
def yearly_csv_url(year: int) -> str:
    return (
        "https://data.stadt-zuerich.ch/dataset/"
        "ted_taz_verkehrszaehlungen_werte_fussgaenger_velo/download/"
        f"{year}_verkehrszaehlungen_werte_fussgaenger_velo.csv"
    )


_NOMINATIM_UA = "velo-zh-monitor/1.0 (github.com/tt-404/velomap)"


def reverse_geocode_street(lat: float, lon: float) -> str | None:
    """Strassenname via Nominatim (OSM). Hält Nominatim-Limit von 1 req/s ein."""
    url = (
        f"https://nominatim.openstreetmap.org/reverse"
        f"?lat={lat}&lon={lon}&format=json&zoom=17&accept-language=de"
    )
    try:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url, headers={"User-Agent": _NOMINATIM_UA})
            r.raise_for_status()
            addr = r.json().get("address", {})
        return (
            addr.get("road")
            or addr.get("pedestrian")
            or addr.get("path")
            or addr.get("cycleway")
            or addr.get("footway")
        )
    except Exception as e:
        log.warning("Nominatim fehlgeschlagen (%.5f, %.5f): %s", lat, lon, e)
        return None


# === Standorte ===
def fetch_stations() -> list[dict]:
    """Standorte als GeoJSON-Features herunterladen."""
    log.info("Lade Standorte von %s", STATIONS_GEOJSON_URL)
    with httpx.Client(timeout=60.0) as client:
        r = client.get(STATIONS_GEOJSON_URL)
        r.raise_for_status()
        gj = r.json()
    return gj.get("features", [])


def upsert_stations(features: Iterable[dict]) -> int:
    """Standorte in DB einfügen / aktualisieren. Holt Strassennamen via Nominatim."""
    n = 0
    needs_geocode: list[int] = []

    with get_session() as s:
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates")
            if not coords:
                continue

            # GeoJSON ist hier WGS84 → (lon, lat)
            lon, lat = float(coords[0]), float(coords[1])

            sid = props.get("id1") or props.get("ID1") or props.get("fk_zaehler")
            if sid is None:
                continue
            sid = int(sid)

            existing = s.get(Station, sid)
            is_new = existing is None
            if is_new:
                existing = Station(id=sid)
                s.add(existing)
                n += 1
            existing.name = props.get("bezeichnun") or props.get("bezeichnung") or f"Station {sid}"
            existing.abkuerzung = props.get("abkuerzung") or ""
            existing.lat = lat
            existing.lon = lon
            existing.bezeichnung = existing.name

            if is_new or not existing.street_name:
                needs_geocode.append(sid)

    log.info("Standorte upserted: %d neu, %d brauchen Geocoding", n, len(needs_geocode))

    # Nominatim-Lookup: 1 req/s einhalten
    geocoded = 0
    for sid in needs_geocode:
        with get_session() as s:
            st = s.get(Station, sid)
            if st is None:
                continue
            lat, lon = st.lat, st.lon
        time.sleep(1.1)
        street = reverse_geocode_street(lat, lon)
        if street:
            with get_session() as s:
                st = s.get(Station, sid)
                if st:
                    st.street_name = street
                    geocoded += 1

    log.info("Strassennamen per Nominatim geholt: %d", geocoded)
    return n


# === Zählwerte ===
# Mapping: Logischer Name -> Liste möglicher CSV-Spaltennamen
# (case-insensitive Matching; Stadt Zürich ändert das Schema ab und zu)
COL_ALIASES = {
    "fk_zaehler":  ["FK_ZAEHLER", "ZAEHLER", "ZAEHLER_ID", "ID_ZAEHLER", "fk_zaehler"],
    "fk_standort": ["FK_STANDORT", "STANDORT", "STANDORT_ID", "ID_STANDORT", "fk_standort"],
    "datum":       ["DATUM", "DATETIME", "TIMESTAMP", "ZEIT", "datum"],
    "velo_in":     ["VELO_IN", "VELO_EIN", "velo_in"],
    "velo_out":    ["VELO_OUT", "VELO_AUS", "velo_out"],
    "fuss_in":     ["FUSS_IN", "FUSSGAENGER_IN", "FUSSG_IN", "FUSS_EIN", "fuss_in"],
    "fuss_out":    ["FUSS_OUT", "FUSSGAENGER_OUT", "FUSSG_OUT", "FUSS_AUS", "fuss_out"],
    "ost":         ["OST", "OST_LV95", "E", "X", "ost"],
    "nord":        ["NORD", "NORD_LV95", "N", "Y", "nord"],
}
REQUIRED_LOGICAL = ["fk_standort", "datum", "velo_in", "velo_out"]


def _resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    """Findet zu jedem logischen Namen die tatsächliche Spalte (case-insensitive).

    Gibt ein Mapping logischer_name -> tatsächlicher_spaltenname zurück.
    Wirft ValueError, wenn eine der REQUIRED_LOGICAL Spalten fehlt.
    """
    # Case-insensitive Lookup: lowercase -> tatsächlicher Name
    lower_map = {c.lower(): c for c in df.columns}
    resolved = {}
    for logical, aliases in COL_ALIASES.items():
        for alias in aliases:
            if alias.lower() in lower_map:
                resolved[logical] = lower_map[alias.lower()]
                break
    missing = [k for k in REQUIRED_LOGICAL if k not in resolved]
    if missing:
        raise ValueError(
            f"Fehlende Pflichtspalten im CSV (gesucht: {missing}). "
            f"Tatsächlich vorhanden: {list(df.columns)}"
        )
    return resolved


def fetch_year_csv(year: int) -> pd.DataFrame:
    """Lädt das Jahres-CSV und gibt ein DataFrame zurück."""
    url = yearly_csv_url(year)
    log.info("Lade Zählwerte %s", url)
    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    log.info("CSV geladen: %d Zeilen, Spalten: %s", len(df), list(df.columns))
    return df


def df_to_records(df: pd.DataFrame, since: datetime | None = None) -> list[dict]:
    """Pandas-DataFrame in Listen von Dicts (für Bulk-Insert) konvertieren."""
    cols = _resolve_columns(df)
    log.info("Spalten-Mapping: %s", cols)

    df = df.copy()
    # Auf logische Namen umbenennen, damit wir konsistent zugreifen können
    rename_map = {v: k for k, v in cols.items()}
    df = df.rename(columns=rename_map)

    # Datum parsen
    df["datum"] = pd.to_datetime(df["datum"], errors="coerce")
    df = df.dropna(subset=["datum", "fk_standort"])
    if since is not None:
        df = df[df["datum"] >= pd.Timestamp(since)]

    has_fuss_in = "fuss_in" in df.columns
    has_fuss_out = "fuss_out" in df.columns
    has_zaehler = "fk_zaehler" in df.columns

    records = []
    for r in df.itertuples(index=False):
        d = r._asdict()
        station_id = int(d["fk_standort"])
        # Wenn FK_ZAEHLER fehlt (neues Schema): nutze station_id als zaehler-Surrogat
        zaehler = str(d["fk_zaehler"]) if has_zaehler else f"Z{station_id}"
        records.append({
            "station_id": station_id,
            "zaehler": zaehler,
            "ts": d["datum"].to_pydatetime(),
            "velo_in": _int_or_none(d.get("velo_in")),
            "velo_out": _int_or_none(d.get("velo_out")),
            "fuss_in": _int_or_none(d.get("fuss_in")) if has_fuss_in else None,
            "fuss_out": _int_or_none(d.get("fuss_out")) if has_fuss_out else None,
        })
    return records


def _int_or_none(v) -> int | None:
    try:
        if pd.isna(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def ensure_stations_from_counts(df: pd.DataFrame) -> int:
    """Falls Standorte aus GeoJSON fehlen, leite sie aus dem CSV ab (LV95→WGS84).

    Setzt voraus, dass OST/NORD-Spalten existieren. Wenn nicht (neuere CSVs
    lassen die Geo-Spalten manchmal weg), wird einfach übersprungen — Standorte
    müssen dann vom GeoJSON kommen.
    """
    try:
        cols = _resolve_columns(df)
    except ValueError:
        return 0

    if "ost" not in cols or "nord" not in cols:
        log.info("CSV enthält keine OST/NORD-Spalten – überspringe Koordinaten-Fallback")
        return 0

    df = df.rename(columns={v: k for k, v in cols.items()})
    n_new = 0
    with get_session() as s:
        first_per_station = df.groupby("fk_standort").first().reset_index()
        for r in first_per_station.itertuples(index=False):
            d = r._asdict()
            try:
                sid = int(d["fk_standort"])
            except (TypeError, ValueError):
                continue
            if s.get(Station, sid) is not None:
                continue
            try:
                lat, lon = lv95_to_wgs84(float(d["ost"]), float(d["nord"]))
            except Exception:
                continue
            s.add(Station(
                id=sid,
                name=f"Station {sid}",
                lat=lat, lon=lon,
                ost=float(d["ost"]), nord=float(d["nord"]),
            ))
            n_new += 1
    log.info("Standorte aus CSV-Koordinaten ergänzt: %d", n_new)
    return n_new


def bulk_insert_counts(records: list[dict]) -> int:
    """Mit ON CONFLICT DO NOTHING (SQLite UNIQUE-Index) bulk-inserten."""
    if not records:
        return 0
    inserted = 0
    CHUNK = 5000
    with get_session() as s:
        for i in range(0, len(records), CHUNK):
            chunk = records[i:i + CHUNK]
            stmt = sqlite_insert(Count).values(chunk)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["zaehler", "station_id", "ts"]
            )
            result = s.execute(stmt)
            inserted += result.rowcount or 0
    return inserted


# === Orchestrierung ===
def run_ingest(initial: bool = False) -> dict:
    """Hauptfunktion: lädt Standorte + aktuelle Zählwerte und schreibt sie in die DB."""
    init_db()
    log_entry = {"source": "stadt-zuerich-opendata", "status": "ok", "rows": 0, "msg": ""}

    # 1) Standorte (via GeoJSON, mit Fallback)
    try:
        features = fetch_stations()
        upsert_stations(features)
    except Exception as e:
        log.warning("Standorte-GeoJSON-Fetch fehlgeschlagen (%s) – fallback auf CSV-Koordinaten", e)

    # 2) Zählwerte
    today = datetime.now()
    current_year = today.year

    if initial:
        years = [current_year - 1, current_year]
    else:
        # Tägliches Update – nur das aktuelle Jahr ist relevant
        years = [current_year]

    total_inserted = 0
    for year in years:
        try:
            df = fetch_year_csv(year)
        except httpx.HTTPStatusError as e:
            log.warning("Jahr %s: HTTP %s – überspringe", year, e.response.status_code)
            continue
        except Exception as e:
            log.error("Jahr %s: Fehler beim Laden – %s", year, e)
            continue

        ensure_stations_from_counts(df)

        # Nur Daten der letzten ~10 Tage importieren (für tägliche Läufe);
        # initial: gesamten Datensatz.
        if not initial:
            since = datetime.now() - timedelta(days=10)
            records = df_to_records(df, since=since)
        else:
            records = df_to_records(df)

        inserted = bulk_insert_counts(records)
        total_inserted += inserted
        log.info("Jahr %s: %d Zeilen geschrieben", year, inserted)

    log_entry["rows"] = total_inserted
    log_entry["msg"] = f"{total_inserted} neue Datensätze"

    # Persistiere Lauf-Log
    with get_session() as s:
        s.add(IngestLog(
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            rows_inserted=total_inserted,
            source=log_entry["source"],
            status=log_entry["status"],
            message=log_entry["msg"],
        ))
    return log_entry


# === CLI ===
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial", action="store_true",
                        help="Initial-Import: aktuelles + Vorjahr")
    args = parser.parse_args()
    res = run_ingest(initial=args.initial)
    print(res)


if __name__ == "__main__":
    main()
