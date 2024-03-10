from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time
import os

spark = SparkSession.builder \
            .appName("scala_udf") \
            .master("local[*]") \
            .config('spark.jars', 'src/resources/exo4/udf.jar') \
            .getOrCreate()

def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    start = time.time()
    
    input_path = "src/resources/exo4/sell.csv/" 
    files = os.listdir(input_path)
    original_files = [file for file in files if "copie" not in file.lower() and file.endswith(".csv")]
    for file in original_files:
        file_path = os.path.join(input_path, file)
        df = spark.read.option("header", "true").option("delimiter", ",").csv(file_path)

    df.withColumn("category_name", addCategoryName(df["category"]))
    output_path = "data/exo4/scala_udf/"
    df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)
    end = time.time()
    print(f"Temps : {end-start} secondes")
