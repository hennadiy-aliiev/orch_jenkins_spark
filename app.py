from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

from dependencies.config.hosts import KAFKA_HOST, HDFS_HOST
from dependencies.schemas import kafka_schema

appName = "PySpark Example"
master = "local[*]"
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

