# Pipeline Open-Meteo (asynchrone)

Ce projet contient un extracteur asynchrone Open-Meteo pour récupérer, pour jusqu'à **10 000 villes**, toutes les variables horaires demandées depuis:

- `https://api.open-meteo.com/v1/forecast`

## Fichiers implémentés

- `src/ingestion/common/base_extractor.py` : classe abstraite `BaseExtractor` (contrat commun pour les extracteurs).
- `src/ingestion/common/retry.py` :
  - décorateur `with_retry` (backoff exponentiel + jitter),
  - circuit breaker (`CircuitBreaker`) pour éviter d'insister sur un service en erreur.
- `src/ingestion/open_meteo/extractor.py` : extracteur Open-Meteo asynchrone (requêtes HTTP via `urllib` + orchestration `asyncio`) avec concurrence configurable.

## Variables extraites

L'extracteur demande exactement les variables horaires suivantes (mapping Open-Meteo):

`temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature, precipitation_probability, precipitation, rain, showers, snowfall, snow_depth, weather_code, pressure_msl, surface_pressure, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, visibility, evapotranspiration, et0_fao_evapotranspiration, vapour_pressure_deficit, wind_speed_10m, wind_speed_80m, wind_speed_120m, wind_speed_180m, wind_direction_10m, wind_direction_80m, wind_direction_120m, wind_direction_180m, wind_gusts_10m, temperature_80m, temperature_120m, temperature_180m, soil_temperature_0cm, soil_temperature_6cm, soil_temperature_18cm, soil_temperature_54cm, soil_moisture_0_to_1cm, soil_moisture_1_to_3cm, soil_moisture_3_to_9cm, soil_moisture_9_to_27cm, soil_moisture_27_to_81cm`

## Exécution

Depuis `pipeline_test/`:

```bash
PYTHONPATH=. python src/ingestion/open_meteo/extractor.py \
  --cities-csv data/cities_10000.csv \
  --output-dir data/bronze \
  --start-date 2025-01-01 \
  --end-date 2025-01-01 \
  --concurrency 10 \
  --max-rounds 0 \
  --api-url https://api.open-meteo.com/v1/forecast
```

## Sortie

- Fichier Parquet: `data/bronze/openmeteo_hourly_<start>_<end>.parquet`
- Format colonne (1 table Parquet) avec une ligne par observation horaire (ville + timestamp + variables).

## Notes perf

- Le paramètre `--concurrency` permet d'ajuster le parallélisme (10 conseillé pour démarrer afin d'éviter les 429).
- Le retry + circuit breaker augmente la robustesse face aux erreurs transitoires (`429`, `5xx`, timeout, etc.).


## Tester rapidement en local

Tu peux valider le pipeline sans dépendre d'Internet avec un faux serveur HTTP local (endpoint compatible).
L'extracteur supporte `--api-url` pour ça.


## Gestion des erreurs 429 (Too Many Requests)

Si tu vois des `HTTP Error 429`, baisse `--concurrency` (ex: 5 ou 10).
Le retry applique un backoff exponentiel et respecte `Retry-After` quand l'API le renvoie.
Les villes en 429 sont replanifiées par vagues avec une phase de cooldown globale pour éviter de bloquer définitivement le run.
