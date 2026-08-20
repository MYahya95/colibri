"""CLI, Spark session, CSV ingest, run order, SQLite write (replaces tables each run)."""

import argparse
import os
import sqlite3
import sys
from glob import glob
from pathlib import Path

from pyspark.sql import SparkSession

from colibri.anomalies import detect_anomalous_turbines
from colibri.cleaning import RAW_SCHEMA, clean_readings
from colibri.stats import summarise_power


def build_spark(app_name: str = "colibri") -> SparkSession:
    _ensure_java()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _ensure_java() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    jdk = Path(__file__).resolve().parents[2] / ".jdk"
    java = next(jdk.rglob("bin/java.exe"), None) if jdk.exists() else None
    if java:
        os.environ["JAVA_HOME"] = str(java.parent.parent)
        os.environ["PATH"] = str(java.parent) + os.pathsep + os.environ.get("PATH", "")


def read_csvs(spark: SparkSession, input_dir: Path):
    paths = [Path(p).resolve().as_posix() for p in glob(str(input_dir / "data_group_*.csv"))]
    if not paths:
        raise FileNotFoundError(f"No data_group_*.csv files in {input_dir}")
    return spark.read.option("header", "true").schema(RAW_SCHEMA).csv(paths)


def write_sqlite(cleaned, summaries, anomalies, db_path: Path) -> dict[str, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tables = {
        "cleaned_readings": cleaned.toPandas(),
        "turbine_period_stats": summaries.toPandas(),
        "turbine_anomalies": anomalies.toPandas(),
    }
    with sqlite3.connect(db_path) as conn:
        for name, pdf in tables.items():
            pdf.to_sql(name, conn, if_exists="replace", index=False)
    return {name: len(pdf) for name, pdf in tables.items()}


def run_pipeline(spark: SparkSession, input_dir: Path, output_dir: Path) -> dict[str, int]:
    cleaned = clean_readings(read_csvs(spark, input_dir))
    summaries = summarise_power(cleaned)
    anomalies = detect_anomalous_turbines(summaries)
    return write_sqlite(cleaned, summaries, anomalies, output_dir / "turbine_analytics.db")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path, default=Path("output"))
    args = parser.parse_args(argv)
    spark = build_spark()
    try:
        counts = run_pipeline(spark, args.input.resolve(), args.output.resolve())
        print(
            f"Wrote {counts['cleaned_readings']} cleaned rows, "
            f"{counts['turbine_period_stats']} daily stats, "
            f"{counts['turbine_anomalies']} anomalies to {args.output}"
        )
    finally:
        spark.stop()
