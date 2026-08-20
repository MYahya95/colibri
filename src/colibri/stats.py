"""Daily summary stats: min / max / average power per turbine per calendar day."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def summarise_power(cleaned: DataFrame) -> DataFrame:
    """Min / max / average MW per turbine per calendar day."""
    return (
        # Brief's "e.g. 24 hours" as a UTC calendar day, not a rolling window.
        cleaned.withColumn("period_start", F.date_trunc("day", "timestamp"))
        .groupBy("turbine_id", "period_start")
        .agg(
            F.min("power_output").alias("min_power_mw"),
            F.max("power_output").alias("max_power_mw"),
            F.round(F.avg("power_output"), 4).alias("avg_power_mw"),
        )
    )
