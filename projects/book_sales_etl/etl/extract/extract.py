from pyspark.sql import SparkSession

def extract(spark: SparkSession, path: str) -> None:
  return spark.read.option("header", True).csv(path)