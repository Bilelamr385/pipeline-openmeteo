# Pipeline Open-Meteo (asynchrone)

Extracteur asynchrone Open-Meteo pour récupérer les variables horaires sur de grands volumes (jusqu'à 10 000 villes).

## Exécution

Depuis `pipeline_test/`:

```bash
PYTHONPATH=. python3 src/ingestion/open_meteo/extractor.py \
  --cities-csv data/cities_10000.csv \
  --output-dir data/bronze \
  --start-date 2025-01-01 \
  --end-date 2025-01-01 \
  --concurrency 1 \
  --requests-per-second 1.0 \
  --max-rounds 0 \
  --api-url https://archive-api.open-meteo.com/v1/archive
```

## Sortie

- Fichier Parquet: `data/bronze/openmeteo_hourly_<start>_<end>.parquet`
- Une ligne par observation horaire (ville + timestamp + variables)

## Robustesse / 429

- Retry exponentiel + jitter sur les erreurs retryables.
- Les villes en `429` sont replanifiées par vagues (`rounds`) jusqu'à succès (`--max-rounds 0`).
- Cooldown global entre rounds pour laisser l'API récupérer.
- Limitation applicative du débit avec `--requests-per-second`.
- Circuit breaker **désactivé par défaut** (option `--use-circuit-breaker`).

## Important

- `pyarrow` est obligatoire pour écrire le Parquet.
- Installation minimale:

```bash
python3 -m pip install pyarrow
```
