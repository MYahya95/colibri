"""Anomalies: flag turbines whose daily average MW is outside 2σ of the farm mean."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

Z_THRESHOLD = 2.0


def detect_anomalous_turbines(summaries: DataFrame, z_threshold: float = Z_THRESHOLD) -> DataFrame:
    """Flag turbine-days whose average MW is more than 2 sample σ from the farm mean that day."""
    # Compare turbines to each other on that day (not vs a power curve).
    farm = Window.partitionBy("period_start")
    mean = F.avg("avg_power_mw").over(farm)
    std = F.stddev_samp("avg_power_mw").over(farm)  # sample σ (n-1), not population
    z = F.when(std > 0, (F.col("avg_power_mw") - mean) / std)  # all equal → σ=0 → no flags
    return (
        summaries.withColumn("z_score", F.round(z, 4))
        .where(F.abs("z_score") > z_threshold)
        .select("turbine_id", "period_start", "avg_power_mw", "z_score")
    )
