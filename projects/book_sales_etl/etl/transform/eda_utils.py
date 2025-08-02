from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType

def handle_missing_values(df: DataFrame, columns):
  for col in columns:
    df[col].fillna("Unknown", inplace=True)

# fuction จัดการกับ outlier
def remove_outliers(df: DataFrame):
  """
    ลบ outliers โดยอัตโนมัติจากทุกคอลัมน์ที่เป็นตัวเลข ด้วย IQR method
  """
  numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    
  for col in numeric_cols:
      q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
      iqr = q3 - q1
      lower_bound = q1 - 1.5 * iqr
      upper_bound = q3 + 1.5 * iqr
      df = df.filter((df[col] >= lower_bound) & (df[col] <= upper_bound))
    
  return df