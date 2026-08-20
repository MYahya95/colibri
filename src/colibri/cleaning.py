"""Cleaning: parse types, drop unusable rows, clip impossible power, impute nulls."""

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# These CSVs never exceed ~4.5 MW; 10 is a hard "not a real reading" cap, not IQR.
POWER_MAX_MW = 10.0

# Read as strings so values like "bad" survive ingest; we coerce them below.
RAW_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True),
    ]
)

CLEAN_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)


def clean_readings(raw: DataFrame) -> DataFrame:
    df = (
        raw.withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        # try_cast: junk like "bad" becomes null. Spark 4 plain cast would fail the job.
        .withColumn("turbine_id", F.expr("try_cast(turbine_id as int)"))
        .withColumn("wind_speed", F.expr("try_cast(wind_speed as double)"))
        .withColumn("wind_direction", F.expr("try_cast(wind_direction as double)"))
        .withColumn("power_output", F.expr("try_cast(power_output as double)"))
        .where(F.col("timestamp").isNotNull() & F.col("turbine_id").isNotNull())
    )
    # Out of range becomes null (impute later); the hour is kept.
    df = df.withColumn(
        "power_output",
        F.when(F.col("power_output").between(0, POWER_MAX_MW), F.col("power_output")),
    )
    # One row per turbine-hour if the CSV repeated a timestamp.
    df = df.groupBy("turbine_id", "timestamp").agg(
        F.avg("wind_speed").alias("wind_speed"),
        F.avg("wind_direction").alias("wind_direction"),
        F.avg("power_output").alias("power_output"),
    )
    return _impute_nulls(df)


def _impute_nulls(df: DataFrame) -> DataFrame:
    # Small PoC volume: interpolate on the driver. Does not insert skipped hours.
    rows = [r.asDict() for r in df.collect()]
    if not rows:
        return df.sparkSession.createDataFrame([], CLEAN_SCHEMA)
    pdf = pd.DataFrame.from_records(rows).sort_values(["turbine_id", "timestamp"])
    cols = ["wind_speed", "wind_direction", "power_output"]
    pdf[cols] = pdf.groupby("turbine_id", group_keys=False)[cols].apply(_fill_numeric)
    return df.sparkSession.createDataFrame(pdf[list(CLEAN_SCHEMA.names)], CLEAN_SCHEMA)


def _fill_numeric(frame: pd.DataFrame) -> pd.DataFrame:
    """Linear interpolation along time; edges take the nearest valid value."""
    filled = frame.apply(lambda col: pd.to_numeric(col, errors="coerce"))
    interpolated = filled.interpolate(method="linear", axis=0, limit_direction="both")
    return pd.DataFrame(interpolated, columns=filled.columns, index=filled.index)
