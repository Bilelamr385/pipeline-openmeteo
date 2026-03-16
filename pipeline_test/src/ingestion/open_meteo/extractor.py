"""Asynchronous Open-Meteo extractor for large city batches."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from src.ingestion.common.base_extractor import BaseExtractor
from src.ingestion.common.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
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


@dataclass(frozen=True)
class ExtractionResult:
    status: Literal["ok", "retry", "failed", "blocked"]
    city: City
    records: list[dict[str, Any]]


class OpenMeteoExtractor(BaseExtractor):
    """Extract hourly weather data from Open-Meteo for a city list."""

    def __init__(
        self,
        cities_csv: Path,
        output_dir: Path,
        start_date: date,
        end_date: date,
        concurrency: int = 3,
        request_timeout_s: float = 30.0,
        api_url: str = "https://api.open-meteo.com/v1/forecast",
        max_rounds: int = 0,
        use_circuit_breaker: bool = False,
        requests_per_second: float = 1.0,
    ) -> None:
        super().__init__(output_dir=output_dir)
        self.cities_csv = cities_csv
        self.start_date = start_date
        self.end_date = end_date
        self.concurrency = concurrency
        self.request_timeout_s = request_timeout_s
        self.api_url = api_url
        self.max_rounds = max_rounds
        self.use_circuit_breaker = use_circuit_breaker
        self.requests_per_second = max(0.1, requests_per_second)
        self.circuit_breaker = CircuitBreaker(CircuitBreakerConfig())
        self._request_lock = asyncio.Lock()
        self._next_request_at = 0.0

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

    async def _wait_rate_limit_slot(self) -> None:
        min_interval = 1.0 / self.requests_per_second
        async with self._request_lock:
            now = time.monotonic()
            sleep_for = self._next_request_at - now
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
                now = time.monotonic()
            self._next_request_at = now + min_interval

    def _fetch_city_sync(self, city: City) -> dict[str, Any]:
        query = urlencode(
            {
                "latitude": city.latitude,
                "longitude": city.longitude,
                "hourly": ",".join(OPEN_METEO_HOURLY_PARAMS),
                "timezone": "UTC",
                "start_date": self.start_date.isoformat(),
                "end_date": self.end_date.isoformat(),
            }
        )
        url = f"{self.api_url}?{query}"
        with urlopen(url, timeout=self.request_timeout_s) as response:  # noqa: S310
            payload = response.read().decode("utf-8")
            return json.loads(payload)

    @with_retry(RetryConfig(max_attempts=7, base_delay=2.0, max_delay=120.0))
    async def _fetch_city(self, city: City) -> dict[str, Any]:
        await self._wait_rate_limit_slot()
        return await asyncio.to_thread(self._fetch_city_sync, city)

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

    async def _extract_one(self, city: City, semaphore: asyncio.Semaphore) -> ExtractionResult:
        async with semaphore:
            try:
                if self.use_circuit_breaker:
                    from src.ingestion.common.retry import call_with_circuit_breaker

                    payload = await call_with_circuit_breaker(self.circuit_breaker, self._fetch_city, city)
                else:
                    payload = await self._fetch_city(city)
                return ExtractionResult(status="ok", city=city, records=self._to_records(city, payload))
            except HTTPError as exc:
                if exc.code == 429:
                    return ExtractionResult(status="retry", city=city, records=[])
                logger.warning("City extraction failed city=%s http_status=%s", city.city, exc.code)
                return ExtractionResult(status="failed", city=city, records=[])
            except RuntimeError as exc:
                if "Circuit breaker is open" in str(exc):
                    return ExtractionResult(status="blocked", city=city, records=[])
                logger.exception("City extraction failed city=%s error=%s", city.city, exc)
                return ExtractionResult(status="failed", city=city, records=[])
            except Exception as exc:  # noqa: BLE001
                logger.exception("City extraction failed city=%s error=%s", city.city, exc)
                return ExtractionResult(status="failed", city=city, records=[])

    @staticmethod
    def _open_parquet_writer(output_file: Path):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError(
                "pyarrow est obligatoire pour la sortie parquet. Installe-le: python3 -m pip install pyarrow"
            ) from exc
        return pa, pq, None

    async def extract_full(self) -> Path:
        cities = self._load_cities()
        logger.info("Starting extraction for %s cities", len(cities))

        output_file = self.output_dir / f"openmeteo_hourly_{self.start_date}_{self.end_date}.parquet"
        if output_file.exists():
            output_file.unlink()

        pa, pq, writer = self._open_parquet_writer(output_file)

        pending = cities
        success_count = 0
        failed_count = 0
        round_no = 0
        semaphore = asyncio.Semaphore(self.concurrency)

        while pending:
            round_no += 1
            if self.max_rounds > 0 and round_no > self.max_rounds:
                logger.warning("Reached max_rounds=%s, stopping with %s pending cities", self.max_rounds, len(pending))
                failed_count += len(pending)
                break

            logger.info("Round %s: processing %s cities", round_no, len(pending))
            tasks = [self._extract_one(city, semaphore) for city in pending]
            results = await asyncio.gather(*tasks)

            next_pending: list[City] = []
            batch_records: list[dict[str, Any]] = []
            for result in results:
                if result.status == "ok":
                    success_count += 1
                    batch_records.extend(result.records)
                elif result.status in {"retry", "blocked"}:
                    next_pending.append(result.city)
                else:
                    failed_count += 1

            if batch_records:
                table = pa.Table.from_pylist(batch_records)
                if writer is None:
                    writer = pq.ParquetWriter(output_file, table.schema)
                writer.write_table(table)

            pending = next_pending
            if pending:
                cooldown = min(300, 10 * (2 ** min(round_no - 1, 5)))
                logger.warning(
                    "Rate limited/blocked on %s cities. Cooling down %ss before retry round %s",
                    len(pending),
                    cooldown,
                    round_no + 1,
                )
                await asyncio.sleep(cooldown)

        if writer is not None:
            writer.close()

        logger.info(
            "Extraction done. Output file: %s | success=%s failed=%s pending=%s",
            output_file,
            success_count,
            failed_count,
            len(pending),
        )
        return output_file


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract Open-Meteo hourly weather for cities CSV")
    parser.add_argument("--cities-csv", type=Path, default=Path("data/cities_10000.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--requests-per-second", type=float, default=1.0)
    parser.add_argument("--max-rounds", type=int, default=0, help="0 = retry until all 429 cities succeed")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--api-url", default="https://api.open-meteo.com/v1/forecast")
    parser.add_argument("--use-circuit-breaker", action="store_true", help="Enable circuit breaker (disabled by default)")
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
        api_url=args.api_url,
        max_rounds=args.max_rounds,
        use_circuit_breaker=args.use_circuit_breaker,
        requests_per_second=args.requests_per_second,
    )
    await extractor.extract_full()


if __name__ == "__main__":
    asyncio.run(_run_from_cli())
