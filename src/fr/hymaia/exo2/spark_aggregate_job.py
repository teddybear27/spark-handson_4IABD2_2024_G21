##############################
#                            #
#   poetry run aggregate     #
#                            #
##############################


from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
import os
import glob

# Fonction pour lire les données à partir d'un fichier Parquet crée précedemment avec clean
def read_parquet_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.option("header", "true").option("delimiter", ",").parquet(file_path)

# Fonction pour effectuer le traitement et le tri des données
def process_and_sort_data(df: DataFrame) -> DataFrame:
    df_result = df.groupBy("departement").agg({"name": "count"}).withColumnRenamed("count(name)", "nb_people")
    return df_result.orderBy(col("nb_people").desc(), col("departement"))

# Fonction pour sauvegarder les résultats au format CSV
def save_results_to_csv(df: DataFrame, output_path: str):
    df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)

# Fonction pour renommer le fichier de sortie
def rename_output_file(output_path: str):
    liste_fichiers = glob.glob(f"{output_path}/*.csv")
    if liste_fichiers:
        spark_filename = liste_fichiers[-1]
        os.rename(spark_filename, os.path.join(output_path, "aggregate.csv"))
    else:
        print("Aucun fichier CSV n'a été généré par Spark.")

# Fonction pour supprimer les fichiers autres que ceux se terminant par ".csv"
def delete_non_csv_files(directory: str):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith(".csv"):
                os.remove(os.path.join(root, file))

def main():
    spark = SparkSession.builder \
        .appName("spark_aggregate_job") \
        .master("local[*]") \
        .getOrCreate()

    input_path = "data/exo2/output/clean.parquet"
    output_path = "data/exo2/output/csv"

    # Lecture des données
    df_first_job = read_parquet_data(spark, input_path)

    # Traitement et tri des données
    df_result = process_and_sort_data(df_first_job)

    # Sauvegarde des résultats au format CSV
    save_results_to_csv(df_result, output_path)

    # Renomme le fichier de sortie
    rename_output_file(output_path)

    # Supprime les fichiers autres que ".csv"
    delete_non_csv_files(output_path)
