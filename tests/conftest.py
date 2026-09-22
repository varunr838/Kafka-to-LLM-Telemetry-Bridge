import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession fixture for testing.
    Keeps tests fast by reusing the same local Spark cluster.
    """
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark-local")
        # Lightweight configs for fast testing
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
