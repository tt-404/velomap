"""APScheduler: Velo-Ingest stündlich, Radar alle 10 Min, Parking alle 2 Min."""
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .ingest import run_ingest
from .miv import run_miv_ingest
from .parking import run_parking_ingest
from .radar import run_radar_ingest
from .water import run_water_ingest

log = logging.getLogger("scheduler")

_scheduler: BackgroundScheduler | None = None


def _velo_job():
    log.info("Scheduler: starte Velo-Ingest")
    try:
        res = run_ingest(initial=False)
        log.info("Scheduler: Velo-Ingest beendet – %s", res)
    except Exception as e:
        log.exception("Scheduler: Velo-Ingest fehlgeschlagen: %s", e)


def _water_job():
    log.info("Scheduler: starte Wasser-Ingest")
    try:
        res = run_water_ingest(initial=False)
        log.info("Scheduler: Wasser-Ingest beendet – %s", res)
    except Exception as e:
        log.exception("Scheduler: Wasser-Ingest fehlgeschlagen: %s", e)


def _miv_job():
    log.info("Scheduler: starte MIV-Ingest")
    try:
        res = run_miv_ingest(initial=False)
        log.info("Scheduler: MIV-Ingest beendet – %s", res)
    except Exception as e:
        log.exception("Scheduler: MIV-Ingest fehlgeschlagen: %s", e)


def _parking_job():
    log.info("Scheduler: starte Parking-Ingest")
    try:
        res = run_parking_ingest()
        log.info("Scheduler: Parking-Ingest beendet – %s", res)
    except Exception as e:
        log.exception("Scheduler: Parking-Ingest fehlgeschlagen: %s", e)


def _radar_job():
    log.info("Scheduler: starte Radar-Ingest")
    try:
        res = run_radar_ingest()
        log.info("Scheduler: Radar-Ingest beendet – %s", res)
    except Exception as e:
        log.exception("Scheduler: Radar-Ingest fehlgeschlagen: %s", e)


def start_scheduler() -> BackgroundScheduler:
    """Startet den Scheduler."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    sched = BackgroundScheduler(timezone="Europe/Zurich")

    # Velo-Daten: stündlich um xx:05
    # (Stadt Zürich aktualisiert das CSV täglich nachts, stündliches Pollen ist sicher)
    sched.add_job(_velo_job, CronTrigger(minute=5), id="velo_ingest")

    # Radar: alle 10 Min, leicht versetzt damit die HDF5-Datei sicher fertig ist
    sched.add_job(_radar_job, CronTrigger(minute="2,12,22,32,42,52"), id="radar_ingest")

    # Parking: alle 2 Min (PLS aktualisiert ca. alle 2-5 Min)
    sched.add_job(_parking_job, CronTrigger(minute="*/2"), id="parking_ingest")

    # MIV: täglich um 07:30 (CSV wird morgens aktualisiert)
    sched.add_job(_miv_job, CronTrigger(hour=7, minute=30), id="miv_ingest")

    # Wasser: alle 10 Min (BAFU-Auflösung)
    sched.add_job(_water_job, CronTrigger(minute="5,15,25,35,45,55"), id="water_ingest")

    sched.start()
    _scheduler = sched
    log.info("Scheduler gestartet (Velo: stündlich, Radar: 10 Min, Parking: 2 Min, MIV: täglich, Wasser: 10 Min)")
    return sched


def shutdown_scheduler():
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
