"""Tests for cleaning: impossible power, malformed values, duplicate hours (all numeric cols averaged)."""

from colibri.cleaning import RAW_SCHEMA, clean_readings


def raw_df(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def _by_hour(rows):
    return {(r.turbine_id, str(r.timestamp)[:19]): r for r in rows}


def test_impossible_power_is_imputed(spark):
    rows = [
        ("2022-03-01 00:00:00", "1", "10", "90", "3.0"),
        ("2022-03-01 01:00:00", "1", "10", "90", "99.0"),  # clipped to null, then filled
        ("2022-03-01 02:00:00", "1", "10", "90", "3.0"),
    ]
    mid = _by_hour(clean_readings(raw_df(spark, rows)).collect())[(1, "2022-03-01 01:00:00")]
    assert mid.power_output == 3.0


def test_nulls_are_imputed(spark):
    rows = [
        ("2022-03-01 00:00:00", "1", "10", "90", "2.0"),
        ("2022-03-01 01:00:00", "1", None, "bad", None),  # "bad" cannot cast; treat as missing
        ("2022-03-01 02:00:00", "1", "12", "90", "4.0"),
    ]
    mid = _by_hour(clean_readings(raw_df(spark, rows)).collect())[(1, "2022-03-01 01:00:00")]
    assert mid.wind_speed == 11.0  # halfway 10 → 12
    assert mid.power_output == 3.0  # halfway 2 → 4


def test_duplicate_hours_are_averaged(spark):
    rows = [
        ("2022-03-01 00:00:00", "1", "10", "90", "2.0"),
        ("2022-03-01 00:00:00", "1", "12", "90", "4.0"),
    ]
    out = clean_readings(raw_df(spark, rows)).collect()
    assert len(out) == 1
    assert out[0].wind_speed == 11.0
    assert out[0].wind_direction == 90.0
    assert out[0].power_output == 3.0
