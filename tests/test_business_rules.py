import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()


def test_high_value_flag():
    df = spark.createDataFrame(
        [Row(amount=1000.0), Row(amount=2500.0)]
    )
    result = df.withColumn("is_high_value", col("amount") > 2000.0).collect()

    assert result[0]["is_high_value"] is False
    assert result[1]["is_high_value"] is True
