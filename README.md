# Wind turbine telemetry pipeline

Proof of Concept: ingest daily CSVs, clean, daily min/max/avg MW, flag turbines outside 2σ of the farm mean, write SQLite.

```
data/data_group_*.csv → clean → daily stats → 2σ flags → output/turbine_analytics.db
```

## Design

Linear batch job, one module per brief bullet ('cleaning.py', 'stats.py', 'anomalies.py'), wired in 'pipeline.py'.

CSVs are read as strings. Cleaning parses types, drops rows without a timestamp or turbine id, treats power outside 0–10 MW as missing, averages duplicate hours, then interpolates remaining nulls along time per turbine (no extra hours for gaps). Stats are min, max, and average MW per turbine per UTC calendar day. Anomalies are turbine-days whose daily average sits more than 2 sample σ from that day's farm mean. Each run replaces three SQLite tables: cleaned readings, daily stats, anomalies.

Spark does the tabular work; interpolate and the SQLite write use pandas on the driver, which is fine for a month of hourly data.

## Assumptions

- Hourly readings; a given 'turbine_id' always lives in the same 'data_group_*.csv'.
- Stats period is a UTC calendar day (24h).
- Impossible power (>10 MW or <0) is treated as missing and linearly interpolated from that turbine's neighbouring hours. Skipped hours are left absent (no synthetic grid).
- Anomaly = that day's average MW more than 2 sample σ from the farm mean of daily averages. The March farm is tight, so flags are uncommon; tests add a clear outlier (turbine 6 at 9 MW).
- SQLite is the database for this exercise. Tables are replaced on every run. 'collect()' / 'toPandas()' is acceptable at this volume.

## Run

Needs Python 3.10+ and JDK 17 ('JAVA_HOME', or a gitignored '.jdk' folder).

```bash
python -m venv .venv
.venv\Scripts\activate (WINDOWS)
source .venv/bin/activate (LINUX/macOS)
pip install -e ".[dev]"
python -m colibri --input data --output output
python -m pytest
```

## With more time

- Fill skipped hours on an hourly grid (sensors drop rows, not just null cells).
- Per-turbine IQR (or similar) for outliers that are still inside 0–10 MW.
- Tests for interpolate ('_fill_numeric') without going through Spark.
- Incremental load: process only the new 24h instead of replacing the whole SQLite database.
- Stronger anomaly cases on the real CSVs (the farm is too similar for many 2σ flags).

## Productionising

- Keep interpolate and writes in Spark ('groupBy' + pandas UDF or windows); do not 'collect()' / 'toPandas()' the lake to the driver.
- Land partitioned Parquet/Delta/Iceberg (date, turbine_id); warehouse or JDBC instead of SQLite.
- Expected MW from a power curve (wind speed → power), then 2σ on residuals; also compare a turbine to its own history.
- Daily job (Airflow/Databricks): bronze raw CSV → silver cleaned → gold stats/anomalies; late data and schema checks.
- Alerts when a turbine is flagged or when missing-hour rate spikes; CI via 'python -m pytest'.
