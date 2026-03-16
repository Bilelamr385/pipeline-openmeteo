"""Open-Meteo ingestion package."""

from .extractor import OPEN_METEO_HOURLY_PARAMS, OpenMeteoExtractor

__all__ = ["OpenMeteoExtractor", "OPEN_METEO_HOURLY_PARAMS"]
