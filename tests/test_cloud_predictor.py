import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("WeatherPipelineTests") \
        .master("local[2]") \
        .getOrCreate()

def test_sea_of_clouds_index_perfect_score(spark):
    
    # 1. Use the exact StringType schema from the Silver layer
    schema = StructType([
        StructField("temperature_2m", StringType(), True),
        StructField("dew_point_2m", StringType(), True),
        StructField("cloud_cover_low", StringType(), True),
        StructField("cloud_cover_mid", StringType(), True),
        StructField("cloud_cover_high", StringType(), True),
        StructField("relative_humidity_2m", StringType(), True),
        StructField("wind_speed_10m", StringType(), True)
    ])
    
    # 2. Mock data provided as strings
    mock_data = [("10.0", "9.0", "90.0", "10.0", "10.0", "95.0", "5.0")]
    df = spark.createDataFrame(mock_data, schema)
    
    # 3. Apply the engine-side casting logic required by the Silver schema
    df_transformed = df.withColumn(
        "dew_point_depression",
        F.round(F.col("temperature_2m").cast("double") - F.col("dew_point_2m").cast("double"), 2)
    ).withColumn(
        "score_low_clouds", F.when(F.col("cloud_cover_low").cast("double").between(80, 100), 1).otherwise(0)
    ).withColumn(
        "score_sky_clear", F.when((F.col("cloud_cover_mid").cast("double") <= 20) & (F.col("cloud_cover_high").cast("double") <= 20), 1).otherwise(0)
    ).withColumn(
        "score_humidity", F.when(F.col("relative_humidity_2m").cast("double") >= 85, 1).otherwise(0)
    ).withColumn(
        "score_dew_point", F.when(F.col("dew_point_depression").between(0, 2), 1).otherwise(0)
    ).withColumn(
        "score_wind", F.when(F.col("wind_speed_10m").cast("double") < 8, 1).otherwise(0)
    ).withColumn(
        "sea_of_clouds_index",
        F.col("score_low_clouds") + F.col("score_sky_clear") + F.col("score_humidity") + F.col("score_dew_point") + F.col("score_wind")
    )
    
    result = df_transformed.collect()[0]
    
    # 4. Assert correctness
    assert result["dew_point_depression"] == 1.0
    assert result["sea_of_clouds_index"] == 5