"""Asynchronous Open-Meteo extractor for large city batches."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import httpx

from src.ingestion.common.base_extractor import BaseExtractor
from src.ingestion.common.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
    call_with_circuit_breaker,
    with_retry,
)

logger = logging.getLogger(__name__)

OPEN_METEO_HOURLY_PARAMS: tuple[str, ...] = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "snow_depth",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "evapotranspiration",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit",
    "wind_speed_10m",
    "wind_speed_80m",
    "wind_speed_120m",
    "wind_speed_180m",
    "wind_direction_10m",
    "wind_direction_80m",
    "wind_direction_120m",
    "wind_direction_180m",
    "wind_gusts_10m",
    "temperature_80m",
    "temperature_120m",
    "temperature_180m",
    "soil_temperature_0cm",
    "soil_temperature_6cm",
    "soil_temperature_18cm",
    "soil_temperature_54cm",
    "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm",
    "soil_moisture_3_to_9cm",
    "soil_moisture_9_to_27cm",
    "soil_moisture_27_to_81cm",
)


@dataclass(frozen=True)
class City:
    city: str
    latitude: float
    longitude: float
    country: str


class OpenMeteoExtractor(BaseExtractor):
    """Extract hourly weather data from Open-Meteo for a city list."""

    def __init__(
        self,
        cities_csv: Path,
        output_dir: Path,
        start_date: date,
        end_date: date,
        concurrency: int = 100,
        request_timeout_s: float = 30.0,
    ) -> None:
        super().__init__(output_dir=output_dir)
        self.cities_csv = cities_csv
        self.start_date = start_date
        self.end_date = end_date
        self.concurrency = concurrency
        self.request_timeout_s = request_timeout_s
        self.api_url = "https://api.open-meteo.com/v1/forecast"
        self.retry_config = RetryConfig(max_attempts=5)
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig())
        self._write_lock = asyncio.Lock()

    def _load_cities(self) -> list[City]:
        cities: list[City] = []
        with self.cities_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                cities.append(
                    City(
                        city=row["city"],
                        latitude=float(row["latitude"]),
                        longitude=float(row["longitude"]),
                        country=row["country"],
                    )
                )
        return cities

    @with_retry(RetryConfig(max_attempts=5))
    async def _fetch_city(self, client: httpx.AsyncClient, city: City) -> dict[str, Any]:
        response = await client.get(
            self.api_url,
            params={
                "latitude": city.latitude,
                "longitude": city.longitude,
                "hourly": ",".join(OPEN_METEO_HOURLY_PARAMS),
                "timezone": "UTC",
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
            },
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _to_records(city: City, payload: dict[str, Any]) -> list[dict[str, Any]]:
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        records: list[dict[str, Any]] = []

        for idx, ts in enumerate(times):
            item: dict[str, Any] = {
                "city": city.city,
                "country": city.country,
                "latitude": city.latitude,
                "longitude": city.longitude,
                "timestamp": ts,
            }
            for key in OPEN_METEO_HOURLY_PARAMS:
                values = hourly.get(key, [])
                item[key] = values[idx] if idx < len(values) else None
            records.append(item)
        return records

    async def _extract_one(
        self,
        client: httpx.AsyncClient,
        city: City,
        output_file: Path,
        semaphore: asyncio.Semaphore,
    ) -> None:
        async with semaphore:
            try:
                payload = await call_with_circuit_breaker(self.circuit_breaker, self._fetch_city, client, city)
                records = self._to_records(city, payload)
                async with self._write_lock:
                    with output_file.open("a", encoding="utf-8") as out:
                        for row in records:
                            out.write(json.dumps(row, ensure_ascii=False) + "\n")
            except Exception as exc:  # noqa: BLE001
                logger.exception("City extraction failed city=%s error=%s", city.city, exc)

    async def extract_full(self) -> Path:
        cities = self._load_cities()
        logger.info("Starting extraction for %s cities", len(cities))

        output_file = self.output_dir / f"openmeteo_hourly_{self.start_date}_{self.end_date}.ndjson"
        output_file.write_text("", encoding="utf-8")

        timeout = httpx.Timeout(timeout=self.request_timeout_s)
        limits = httpx.Limits(max_connections=self.concurrency, max_keepalive_connections=self.concurrency)

        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self._extract_one(client, city, output_file, semaphore) for city in cities]
            await asyncio.gather(*tasks)

        logger.info("Extraction done. Output file: %s", output_file)
        return output_file


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract Open-Meteo hourly weather for cities CSV")
    parser.add_argument("--cities-csv", type=Path, default=Path("data/cities_10000.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--log-level", default="INFO")
    return parser


async def _run_from_cli() -> None:
    args = _build_arg_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    extractor = OpenMeteoExtractor(
        cities_csv=args.cities_csv,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        concurrency=args.concurrency,
    )
    await extractor.extract_full()


if __name__ == "__main__":
    asyncio.run(_run_from_cli())
