# Velo-Zürich Monitor

Applikation, die Velo-Zähldaten der Stadt Zürich und Niederschlags-Radardaten von MeteoSwiss automatisch abholt, in einer SQLite-Datenbank archiviert und auf einer interaktiven Karte mit 7-Tage-Zeitstrahl visualisiert.

## Architektur

- **Backend**: Python / FastAPI
  - `ingest.py`: Velo-Zähldaten (CSV) stündlich von data.stadt-zuerich.ch
  - `radar.py`: MeteoSwiss-Niederschlagsradar (CombiPrecip HDF5) alle 10 Min via STAC-API
  - SQLite-DB mit drei Tabellen: `stations`, `counts`, `precip`
- **Frontend**: Leaflet-Karte mit Zeitstrahl-Slider

## Datenquellen

| Quelle | Was | Update | Historie offen |
|---|---|---|---|
| Stadt Zürich Velozählungen | 15-Min-Werte, richtungsgetrennt | täglich | seit 2009 (~16 Jahre) |
| MeteoSwiss CombiPrecip Radar | 1h-Niederschlagssumme HDF5-Raster (1km) | alle 5 Min | rollende 14 Tage |

**Warum lokale Archivierung**: MeteoSwiss stellt Radardaten nur 14 Tage rückwirkend offen bereit. Die App speichert daher bei jedem 10-Min-Lauf den Niederschlagswert am Pixel jeder Velozählstelle in der eigenen DB. So baut sich Schritt für Schritt eine längere Historie auf, die mit den Velo-Daten korreliert werden kann.

## Start

### Mit Docker

```bash
docker compose up --build
```

### Mit Podman

```bash
podman compose up --build
# oder, falls podman-compose separat installiert ist:
podman-compose up --build
```

→ http://localhost:8000

Beim ersten Start lädt die App im Hintergrund die Velo-Standorte und das aktuelle Jahres-CSV (~50 MB). Der erste Radar-Lauf kommt nach max. 10 Minuten.

### Lokal

```bash
cd backend
pip install -r requirements.txt
python -m app.ingest --initial
uvicorn app.main:app --reload
```

## Optional: Street-View-Bilder im Popup

Bei Klick auf einen Marker zeigt das Popup ein Bild von der Strasse (via Mapillary). Damit das Bild direkt angezeigt wird statt nur eines Links, brauchst du einen kostenlosen Mapillary Access Token:

1. Registrieren unter https://www.mapillary.com/dashboard/developers
2. "Register Application" → Access Token kopieren
3. Datei `.env` im Projekt-Root anlegen (oder `.env.example` kopieren):
   ```
   MAPILLARY_TOKEN=MLY|xxxxxxxxxxxxxxxxxxxxxxxxxxxx
   ```
4. Container neu starten

Ohne Token zeigt das Popup einen Link zum Mapillary-Webviewer — der funktioniert auch ohne Anmeldung.

## API-Endpoints

**Velo**
- `GET /api/stations`
- `GET /api/counts?from=ISO&to=ISO&station_id=...`
- `GET /api/snapshot?at=ISO&window=900`
- `GET /api/range`
- `POST /api/ingest/trigger`

**Niederschlag**
- `GET /api/precip?from=ISO&to=ISO&station_id=...`
- `GET /api/precip/snapshot?at=ISO&window=1800`
- `POST /api/ingest/radar`

**System**
- `GET /health` – Status: Anzahl Stationen, Counts, Precip-Werte

## Projektstruktur

```
velo-zh/
├── backend/
│   ├── app/
│   │   ├── main.py          FastAPI + Endpoints
│   │   ├── db.py            SQLite-Schema
│   │   ├── ingest.py        Stadt-Zürich CSV → DB
│   │   ├── radar.py         MeteoSwiss HDF5 → DB
│   │   ├── scheduler.py     APScheduler
│   │   └── coords.py        LV95 ↔ WGS84
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/index.html
├── docker-compose.yml
└── README.md
```

## Lizenz

- **Stadt Zürich**: CC0 (freie Nutzung)
- **MeteoSwiss**: Open Data, Quellenangabe "MeteoSchweiz" erforderlich
