import datetime

from pyspark.sql.types import Row

from app import get_idle_hotel_ids, extract_kafka_data


def test_extract_kafka_data(spark):
    expected = Row(
        id='2388001816576',
        avg_tmpr_c=10.600000381469727,
        avg_tmpr_f=51.0,
        wthr_date=datetime.date(2016, 10, 1),
        day=1,
        month=10,
        year=2016
    )

    row = Row(
        key='None',
        value=bytearray(b'{"id":"2388001816576","name":"11 Cadogan Gardens","avg_tmpr_f":"51.0","avg_tmpr_c":"10.6","wthr_date":"2016-10-01","year":2016,"month":10,"day":1}')
    )
    df = spark.createDataFrame([row])

    result = extract_kafka_data(df)

    assert expected == result.collect()[0]


def test_get_idle_hotel_ids(spark):
    expected = [1, 2, 3]

    data = [
        (1, "value1"),
        (2, "value2"),
        (3, "value3"),
    ]
    df = spark.createDataFrame(data, ["hotel_id", "some_column"])
    result = get_idle_hotel_ids(df)

    assert expected == result

