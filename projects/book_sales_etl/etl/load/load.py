def save_to_parquet(df, output_path):
  
  """ 
  ปกติจะใช้ Parquet เป็นหลักใน pipline การบันทึกข้อมูล
  แต่จะใช้ csv เมื่อเอาไปทำ data analyst / dash board
  """
  # df.write.mode("overwrite").parquet(output_path)
  df.write.mode("overwrite").option("header", True).csv(output_path)