import sys
import os
import yaml

# 📌 เพิ่ม path ของโปรเจกต์ (book_sales_etl/) ให้ Python หา module etl ได้
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# 📌 โหลด config อย่างถูกต้อง
config_path = os.path.join(project_root, "config", "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

from pyspark.sql import SparkSession
from etl.extract.extract import extract
from etl.transform.clean import clean_sales_data
from etl.transform.eda_utils import remove_outliers
from etl.transform.transform import transform
from etl.load.load import save_to_parquet
from utils.logger import get_logger

logger = get_logger("run_etl")

spark = SparkSession.builder.appName("Book_Sales_ETL").getOrCreate()

try:
    logger.info("🚀 Starting ETL process")

    # 🟠 Extract
    logger.info("🔍 Starting data extraction from %s", config["data_source"]["csv_path"])
    df_raw = extract(spark, config["data_source"]["csv_path"])
    row_count = df_raw.count()
    logger.info("✅ Extracted %d rows", row_count)
    logger.debug("Sample schema:\n%s", df_raw.printSchema())

    # 🟡 Clean
    logger.info("🧹 Cleaning missing/null values")
    df_clean = clean_sales_data(df_raw)
    logger.info("✅ Data cleaned: %d rows", df_clean.count())

    # 🔵 Remove outliers
    logger.info("📉 Removing outliers")
    df_on_outliers = remove_outliers(df_clean)
    logger.info("✅ Outliers removed: %d rows", df_on_outliers.count())

    # 🟣 Transform
    logger.info("🔄 Transforming data")
    df_transformed = transform(df_on_outliers)
    logger.info("✅ Transformation completed: %d rows", df_transformed.count())

    # 🟢 Load
    logger.info("💾 Saving transformed data to parquet: %s", config["processed"]["clean_path"])
    save_to_parquet(df_transformed, config["processed"]["clean_path"])
    logger.info("✅ Data saved to parquet")

    logger.info("🎉 ETL process completed successfully")

except Exception as e:
    logger.exception("❌ Error during ETL: %s", str(e))
