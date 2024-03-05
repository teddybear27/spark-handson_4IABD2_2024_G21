from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, date_sub, max, lit
from pyspark.sql.window import Window
import time
import os

def main():
    start = time.time()
    spark = SparkSession.builder.appName("exo4_no_udf").master("local[*]").getOrCreate()

    input_path = "src/resources/exo4/sell.csv/"    

    files = os.listdir(input_path)
    original_files = [file for file in files if "copie" not in file.lower() and file.endswith(".csv")]
    for file in original_files:
        file_path = os.path.join(input_path, file)
        df = spark.read.option("header", "true").option("delimiter", ",").csv(file_path)

    df = df.withColumn("date", col("date").cast("date")) \
           .withColumn("category", col("category").cast("integer")) \
           .withColumn("price", col("price").cast("double")) \
           .withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))
    df_totals = df.groupBy("date", "category_name") \
                  .agg(sum("price").alias("total_price_per_category_per_day"))
    df_final = df.join(df_totals, ["date", "category_name"], "inner")
    df_final.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day")
    output_path_category_per_day = "data/exo4/category_per_day/"
    df_final.write.mode("overwrite").format("csv").option("header", "true").save(output_path_category_per_day)

    max_date = df.agg(max("date").alias("max_date")).collect()[0]["max_date"]
    start_date = date_sub(lit(max_date), 31)
    df_filtered = df.filter(col("date") >= start_date)
    windowSpec = Window.partitionBy("category_name").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_result = df_filtered.withColumn("total_price_per_category_per_day_last_30_days", sum("price").over(windowSpec))

    df_result = df_result.filter(col("date") >= start_date)
    df_result.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days")
    output_path_last_30 = "data/exo4/last_30_days/"
    df_result.write.mode("overwrite").format("csv").option("header", "true").save(output_path_last_30)

    end = time.time()
    print(f"Temps : {end - start} secondes")

