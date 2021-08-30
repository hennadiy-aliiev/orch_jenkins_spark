
import pytest

from app import get_idle_hotel_ids


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

