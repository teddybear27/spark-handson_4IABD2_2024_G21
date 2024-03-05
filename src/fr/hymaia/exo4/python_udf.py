from pyspark.sql.functions import udf, StringType
from pyspark.sql import SparkSession
import time
import os

spark = SparkSession.builder \
            .appName("python_udf") \
            .master("local[*]") \
            .getOrCreate()    

def main():    
    start = time.time()

    def add_category_name(category):
        return "food" if category < 6 else "furniture"
    
    input_path = "src/resources/exo4/sell.csv/" 
    files = os.listdir(input_path)
    original_files = [file for file in files if "copie" not in file.lower() and file.endswith(".csv")]
    for file in original_files:
        file_path = os.path.join(input_path, file)
        df = spark.read.option("header", "true").option("delimiter", ",").csv(file_path)

    add_category_name_udf = udf(add_category_name, StringType())    
    df = df.withColumn("category", df["category"].cast("int"))
    df.withColumn("category_name", add_category_name_udf(df["category"]))
    output_path = "data/exo4/python_udf/"
    df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)

    end = time.time() 
    print(f"Temps : {end-start} secondes")
