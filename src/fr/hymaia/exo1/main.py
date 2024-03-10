##############################
#                            #
#   poetry run wordcount     #
#                            #
##############################


import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import os

# Fonction pour lire les données à partir d'un fichier CSV
def read_csv_data(spark: SparkSession, file_path: str):
    return spark.read.option("header", "true").csv(file_path)

# Fonction pour effectuer le comptage de mots
def wordcounts(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
    
    # local[*] --> cela signifie qu'on va exécuter Spark en mode local avec un nombre de threads égal au nombre de cœurs disponibles sur ma machine
    # local[*] plutôt que local --> cela simplifie le développement et le débogage sur des machines avec différents nombres de cœurs de processeur.

    input_path = "src/resources/exo1/data.csv"
    output_path = "data/exo1/output"

    # Lecture des données CSV
    df = read_csv_data(spark, input_path)

    # Modification du texte en remplaçant les caractères spéciaux
    df_words = df.withColumn("text", f.regexp_replace(df["text"], "[,\\.\\n]", " "))

    # Effectue le comptage de mots
    result = wordcounts(df_words, "text")

    # Sauvegarde les résultats au format Parquet
    result.write.mode("overwrite").format("parquet").option("header", "true").partitionBy("count").save(output_path)

    # Supprime les fichiers autres que ".parquet"
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if not file.endswith(".parquet"):
                os.remove(os.path.join(root, file))
