from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

from dependencies.schemas import kafka_schema

HDFS_HOST = "hdfs://192.168.49.1:9000"
KAFKA_HOST = "10.107.23.188:9092"

appName = "PySpark Example"
master = "local[*]"
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use test_db")


# Read hotels&weather data from Kafka with Spark application in a batch manner
def read_kafka():
    df = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_HOST) \
      .option("subscribePattern", "hotelWeatherHiveToKafka3") \
      .option("startingOffsets", "earliest") \
      .option("endingOffsets", "latest") \
      .load()
    df = df \
      .selectExpr("cast (value as string) as json") \
      .select(from_json("json", kafka_schema).alias("data")) \
      .select("data.id", "data.name", "data.avg_tmpr_c", "data.avg_tmpr_f", "data.wthr_date", "data.day", "data.month", "data.year") \
      .selectExpr("id", "name", "cast (avg_tmpr_c as float) as avg_tmpr_c", "cast (avg_tmpr_f as float) as avg_tmpr_f", "wthr_date", "day", "month", "year")
    return df.drop('name')


def read_hdfs():
    return spark.read.format("avro").load(f"{HDFS_HOST}/expedia")


def get_idle_hotel_ids(df):
    return [item.hotel_id for item in df.collect()]


def main():
    df_kafka_hotel_weather = read_kafka().cache()

    # Read Expedia data from HDFS with Spark.
    df_expedia = read_hdfs()

    # Calculate idle days (days betweeen current and previous check in dates) for every hotel.
    unique_days = df_expedia.select("srch_ci").where("srch_ci is not null").distinct().count()
    df_hotels_with_idle_days = spark.sql(f"select hotel_id, ({unique_days} - count(*)) as idle_days from (select distinct hotel_id, srch_ci from expedia_ext order by hotel_id, srch_ci) where srch_ci is not null group by hotel_id having count(*) < {unique_days}")
    hotel_ids_idle = get_idle_hotel_ids(df_hotels_with_idle_days)

    # Remove all booking data for hotels with at least one "invalid" row
    df_hotels = spark.sql("select * from hotelwithgeohash_int")
    df_expedia = df_expedia.withColumnRenamed('id', 'booking_id')
    df_expedia_hotels = df_expedia.join(df_hotels, df_expedia.hotel_id == df_hotels.id, 'left').drop('id')
    df_expedia_hotels_no_idle = df_expedia_hotels.filter(~df_expedia_hotels.hotel_id.isin(hotel_ids_idle))

    # Print hotels info (name, address, country etc) of "invalid" hotels and make a screenshot. Join expedia and hotel data for this purpose.
    df_idle_hotels_info = df_expedia_hotels.filter(df_expedia_hotels.hotel_id.isin(hotel_ids_idle)).select('hotel_id', 'name', 'country', 'city', 'address', 'latitude', 'longitude', 'geohash').distinct()

    # Group the remaining data and print bookings counts: 1) by hotel country, 2) by hotel city. Make screenshots of the outputs.
    df_expedia_hotels_no_idle.groupBy('country').count().orderBy('count', ascending=False).show()
    df_expedia_hotels_no_idle.groupBy('city').count().orderBy('count', ascending=False).show()

    # Store "valid" Expedia data in HDFS partitioned by year of "srch_ci".
    df_expedia_hotels_no_idle_with_year = df_expedia_hotels_no_idle.selectExpr("*", "year(srch_ci) as srch_ci_year")
    df_expedia_hotels_no_idle_with_year.write.partitionBy('srch_ci_year').parquet(f'{HDFS_HOST}/expedia_hotels_no_idle')


if __name__ == '__main__':
    main()
