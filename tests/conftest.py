"""Shared pytest fixtures: one local Spark session per test."""

import uuid

import pytest

from colibri.pipeline import build_spark


@pytest.fixture
def spark():
    # New session per test: reusing one SparkSession on Windows/PySpark 4 can hang.
    session = build_spark(f"test-{uuid.uuid4().hex[:8]}")
    try:
        yield session
    finally:
        session.stop()
