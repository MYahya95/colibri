"""Tests for 2σ farm-level anomaly flags."""

from datetime import datetime

from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

from colibri.anomalies import detect_anomalous_turbines

SCHEMA = StructType(
    [
        StructField("turbine_id", IntegerType(), False),
        StructField("period_start", TimestampType(), False),
        StructField("avg_power_mw", DoubleType(), False),
    ]
)


def test_flags_outlier_turbine(spark):
    day = datetime(2022, 3, 1)
    # Five turbines at 3 MW and one at 9 MW is enough for |z| > 2; the real month often is not.
    rows = [(i, day, 3.0) for i in range(1, 6)] + [(6, day, 9.0)]
    flagged = detect_anomalous_turbines(spark.createDataFrame(rows, SCHEMA)).collect()
    assert [r.turbine_id for r in flagged] == [6]


def test_no_flag_when_all_equal(spark):
    day = datetime(2022, 3, 1)
    rows = [(i, day, 3.0) for i in range(1, 5)]  # σ = 0 → no 2σ flags
    assert detect_anomalous_turbines(spark.createDataFrame(rows, SCHEMA)).collect() == []
