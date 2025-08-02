from pyspark.sql import DataFrame
from pyspark.sql.functions import month, year, when

def transform(df: DataFrame) -> DataFrame:
    """
    ปรับปรุงคุณภาพของข้อมูล ให้เหมาะกับการวิเคราะห์
    """
    # เพิ่มคอลัมน์ month และ year
    df = df.withColumn("month",month("order_date")).withColumn("year",year("order_date"))

    # เพิ่มคอลัมน์ season
    df = df.withColumn("season", 
    when(df.month.isin(11, 12, 1), "Cool")
    .when(df.month.isin(2, 3, 4), "Hot")
    .when(df.month.isin(5, 6, 7, 8, 9, 10), "Rainy"))
  
    return df
  