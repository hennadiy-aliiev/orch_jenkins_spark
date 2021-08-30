from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    DateType
)


kafka_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("avg_tmpr_c", StringType(), True),
    StructField("avg_tmpr_f", StringType(), True),
    StructField("wthr_date", DateType(), True),
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
])
