import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    appName = "PySpark Tests"
    master = "local[*]"
    ss = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .enableHiveSupport() \
        .getOrCreate()
    return ss
