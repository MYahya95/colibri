"""Tests for daily min / max / average power."""

from datetime import datetime

from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

from colibri.stats import summarise_power

SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), False),
        StructField("turbine_id", IntegerType(), False),
        StructField("power_output", DoubleType(), False),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
    ]
)


def test_daily_min_max_avg(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 2.0, 10.0, 90.0),
        (datetime(2022, 3, 1, 12), 1, 4.0, 10.0, 90.0),
        (datetime(2022, 3, 2, 0), 1, 8.0, 10.0, 90.0),  # next calendar day
    ]
    stats = {(r.turbine_id, str(r.period_start)[:10]): r for r in summarise_power(spark.createDataFrame(rows, SCHEMA)).collect()}
    assert stats[(1, "2022-03-01")].min_power_mw == 2.0
    assert stats[(1, "2022-03-01")].max_power_mw == 4.0
    assert stats[(1, "2022-03-01")].avg_power_mw == 3.0
    assert stats[(1, "2022-03-02")].avg_power_mw == 8.0
