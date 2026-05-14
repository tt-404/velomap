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
