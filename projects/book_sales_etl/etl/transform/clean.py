from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lower, regexp_replace, when, to_date, trim


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    เปลี่ยนชื่อ column ให้เป็น lowercase + snake_case
    """
    for c in df.columns:
        new_name = c.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(c, new_name)
    return df


def cast_column_types(df: DataFrame) -> DataFrame:
    """
    กำหนด dtype ที่ถูกต้องตาม schema ที่ใช้ downstream
    """
    return (
        df.withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("book_title", col("book_title").cast(StringType()))
          .withColumn("category", col("category").cast(StringType()))
          .withColumn("store_location", col("store_location").cast(StringType()))
          .withColumn("quantity", col("quantity").cast(IntegerType()))
          .withColumn("price_per_unit", col("price_per_unit").cast(DoubleType()))
          .withColumn("payment_method", col("payment_method").cast(StringType()))
          .withColumn("order_date", to_date(col("order_date"), "d/M/yyyy"))
          .withColumn("total_price", col("total_price").cast(DoubleType()))
    )


def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    กำหนดค่า default หรือ drop แถวที่ critical column เป็น null

    ค่าที่เป็น ตัวเลข (numeric) เช่น price, quantity
    — ปกติจะเติมด้วยค่าอย่าง 0 หรือค่าเฉลี่ย (mean) เพื่อไม่ให้ข้อมูลหายไป
    — ช่วยให้การคำนวณหรือโมเดลทำงานได้ต่อโดยไม่มี missing values

    ค่าที่เป็น string หรือ วันที่ (date) ที่เป็นข้อมูลสำคัญ
    — ถ้าขาดจริง ๆ อาจต้อง ลบแถวนั้นทิ้ง เพราะข้อมูลพวกนี้มักจะต้องถูกต้องและครบ
    — หรือถ้าไม่สำคัญมาก อาจเติมด้วยค่าเช่น "unknown", "missing" หรือวันที่ default (เช่น 1970-01-01) เพื่อแทนที่ค่าหายไป
    """
    # 1. ลบแถว (row) ที่คอลัมน์สำคัญ มีค่า null (missing)
    df = df.dropna(subset=["order_id", "book_title", "category", "store_location", "payment_method", "order_date"])
    # 2. แทนค่าที่หายไปในคอลัมน์ ด้วย 0
    df = df.fillna({"total_price": 0.0, "price_per_unit": 0.0 ,"quantity": 0})
    return df


def remove_invalid_rows(df: DataFrame) -> DataFrame:
    """
    กรองแถวที่มีค่าไม่สมเหตุผล เช่น ราคาติดลบ ปริมาณติดลบ
    """
    return df.filter((col("quantity") >= 0) & (col("total_price") >= 0) & (col("price_per_unit") >= 0))


def standardize_text(df: DataFrame) -> DataFrame:
    """
    ทำให้ค่าข้อความ standard เช่น lower case, ตัดช่องว่าง
    """
    return (
        df.withColumn("product_name", lower(trim(col("product_name"))))
          .withColumn("category", lower(trim(col("category"))))
    )


def clean_sales_data(df: DataFrame) -> DataFrame:
    """
    รวมทุกขั้นตอนเป็น pipeline เดียว
    """
    df = normalize_column_names(df)
    df = cast_column_types(df)
    df = handle_missing_values(df)
    df = remove_invalid_rows(df)
    # df = standardize_text(df)
    return df
