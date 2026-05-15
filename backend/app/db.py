"""SQLite-Datenbank für Velo-Zähldaten der Stadt Zürich."""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import (
    Column, DateTime, Float, Integer, String, ForeignKey, Index, create_engine, event
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DB_PATH = os.environ.get("VELO_DB_PATH", "./velo.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    pool_pre_ping=True,
)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")   # parallele Reads während Writes
    cur.execute("PRAGMA busy_timeout=8000")  # 8s warten statt sofort locken
    cur.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Station(Base):
    """Eine Velozählstelle (Standort)."""
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True)  # FK_STANDORT
    name = Column(String, nullable=False)
    abkuerzung = Column(String)
    # WGS84 (für Leaflet)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    # Original LV95
    ost = Column(Float)
    nord = Column(Float)
    bezeichnung = Column(String)
    street_name = Column(String)  # Strassenname via Nominatim (OSM)

    counts = relationship("Count", back_populates="station")


class Count(Base):
    """Ein 15-Minuten-Zählwert."""
    __tablename__ = "counts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    zaehler = Column(String)  # FK_ZAEHLER – Gerät (ein Standort kann mehrere haben)
    ts = Column(DateTime, nullable=False)  # lokal CET, Beginn der 15-Min-Periode
    velo_in = Column(Integer)
    velo_out = Column(Integer)
    fuss_in = Column(Integer)
    fuss_out = Column(Integer)

    station = relationship("Station", back_populates="counts")

    __table_args__ = (
        Index("ix_counts_ts", "ts"),
        Index("ix_counts_station_ts", "station_id", "ts"),
        # Verhindert doppelten Import
        Index("uq_counts_natural", "zaehler", "station_id", "ts", unique=True),
    )


class Precip(Base):
    """Niederschlagswert pro Station und Zeitpunkt (aus MeteoSwiss-Radar extrahiert).

    Auflösung: 5 Min (PRECIP-Produkt RZC) bzw. 10 Min (CombiPrecip CPC).
    Einheit: mm/h (RZC, instantan) oder mm (CPC, akkumuliert über 1h).
    """
    __tablename__ = "precip"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    ts = Column(DateTime, nullable=False)  # UTC
    value = Column(Float)  # Niederschlag am nächsten Radar-Pixel
    product = Column(String)  # "RZC" (5-min instantan) oder "CPC" (1h-Summe)

    __table_args__ = (
        Index("ix_precip_ts", "ts"),
        Index("ix_precip_station_ts", "station_id", "ts"),
        Index("uq_precip_natural", "station_id", "ts", "product", unique=True),
    )


class WaterStation(Base):
    """Hydrologische Messstation (BAFU via api.existenz.ch)."""
    __tablename__ = "water_stations"
    id = Column(String, primary_key=True)   # BAFU-Stations-ID, z.B. "2099"
    name = Column(String)                    # z.B. "Zürich Unterhard"
    water_body = Column(String)              # z.B. "Limmat"
    water_type = Column(String)              # "river" | "lake"
    lat = Column(Float)
    lon = Column(Float)
    readings = relationship("WaterReading", back_populates="station")


class WaterReading(Base):
    """10-Minuten-Messwert (Temperatur und/oder Wasserstand)."""
    __tablename__ = "water_readings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey("water_stations.id"), nullable=False)
    ts = Column(DateTime, nullable=False)   # UTC
    temperature = Column(Float)             # °C (kann None sein)
    height = Column(Float)                  # cm (kann None sein)
    station = relationship("WaterStation", back_populates="readings")

    __table_args__ = (
        Index("ix_water_ts", "ts"),
        Index("ix_water_station_ts", "station_id", "ts"),
        Index("uq_water_natural", "station_id", "ts", unique=True),
    )


class MivStation(Base):
    """MIV-Zählstelle (Strassenverkehr, Stadt Zürich)."""
    __tablename__ = "miv_stations"
    id = Column(String, primary_key=True)   # ZSID, z.B. "Z001"
    name = Column(String)                    # ZSName
    street = Column(String)                  # Achse
    road_class = Column(String)              # "national" | "kantonal" | "kommunal"
    lat = Column(Float)
    lon = Column(Float)
    counts = relationship("MivCount", back_populates="station")


class MivCount(Base):
    """Stündlicher MIV-Zählwert pro Richtung."""
    __tablename__ = "miv_counts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, ForeignKey("miv_stations.id"), nullable=False)
    direction = Column(String)               # Richtung, z.B. "auswärts"
    ts = Column(DateTime, nullable=False)    # MessungDatZeit (stündlich)
    count = Column(Integer)                  # AnzFahrzeuge
    station = relationship("MivStation", back_populates="counts")

    __table_args__ = (
        Index("ix_miv_ts", "ts"),
        Index("ix_miv_station_ts", "station_id", "ts"),
        Index("uq_miv_natural", "station_id", "direction", "ts", unique=True),
    )


class ParkingGarage(Base):
    """Parkhaus im Zürcher Parkleitsystem."""
    __tablename__ = "parking_garages"
    slug = Column(String, primary_key=True)   # pid aus PLS-URL, z.B. "accu"
    name = Column(String, nullable=False)
    address = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    readings = relationship("ParkingReading", back_populates="garage")


class ParkingReading(Base):
    """Aktueller Zustand eines Parkhauses (wird bei jedem Ingest überschrieben)."""
    __tablename__ = "parking_readings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    garage_slug = Column(String, ForeignKey("parking_garages.slug"), nullable=False, unique=True)
    ts = Column(DateTime, nullable=False)   # UTC, aus dc:date
    free = Column(Integer)
    status = Column(String)
    garage = relationship("ParkingGarage", back_populates="readings")


class ParkingHistory(Base):
    """Verlauf der Parkhaus-Belegung — neuer Eintrag nur bei Wertänderung."""
    __tablename__ = "parking_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    garage_slug = Column(String, ForeignKey("parking_garages.slug"), nullable=False)
    ts = Column(DateTime, nullable=False)   # UTC, unser Poll-Zeitpunkt
    free = Column(Integer)
    status = Column(String)

    __table_args__ = (
        Index("ix_parking_history_ts", "ts"),
        Index("ix_parking_history_garage_ts", "garage_slug", "ts"),
    )


class IngestLog(Base):
    """Protokoll der Ingestion-Läufe."""
    __tablename__ = "ingest_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    rows_inserted = Column(Integer, default=0)
    source = Column(String)  # "velo-zh" | "meteoswiss-radar"
    status = Column(String)  # "ok" | "error" | "skipped"
    message = Column(String)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
