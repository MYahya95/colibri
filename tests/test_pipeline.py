"""End-to-end test: fixture CSVs through the pipeline into SQLite."""

import sqlite3
from pathlib import Path

from colibri.pipeline import run_pipeline

FIXTURE_DIR = Path(__file__).parent / "fixtures"  # dirty mini-CSVs, including turbine 6 at 9 MW


def test_pipeline_writes_sqlite(spark, tmp_path):
    counts = run_pipeline(spark, FIXTURE_DIR, tmp_path)  # tmp_path so we don't touch output/
    db = tmp_path / "turbine_analytics.db"
    assert counts["turbine_anomalies"] >= 1
    assert db.exists()
    with sqlite3.connect(db) as conn:
        ids = [r[0] for r in conn.execute("SELECT turbine_id FROM turbine_anomalies")]
    assert 6 in ids
