"""LV95 (EPSG:2056) → WGS84 Konvertierung mittels pyproj."""
from pyproj import Transformer

# always_xy: Eingabe (ost, nord), Ausgabe (lon, lat)
_TRANS = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True)


def lv95_to_wgs84(ost: float, nord: float) -> tuple[float, float]:
    """Konvertiert Schweizer LV95-Koordinaten in (lat, lon)."""
    lon, lat = _TRANS.transform(ost, nord)
    return lat, lon
