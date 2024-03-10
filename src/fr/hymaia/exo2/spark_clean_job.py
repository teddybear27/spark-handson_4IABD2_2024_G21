##########################
#                        #
#   poetry run clean     #
#                        #
##########################


from pyspark.sql.functions import when, col, expr
from pyspark.sql import SparkSession, DataFrame
import os
import glob

# Fonction pour lire les données des clients
def read_clients_data(spk: SparkSession, input_path: str) -> DataFrame:
    return spk.read.option("header", "true").option("delimiter", ",").csv(input_path)

# Fonction pour filtrer les clients majeurs
def filter_major_clients(df: DataFrame) -> DataFrame:
    return df.filter(df["age"] >= 18)

# Fonction pour obtenir les informations des clients avec les villes
def get_clients_with_cities(spk: SparkSession, df_major_clients: DataFrame, df_villes:DataFrame) -> DataFrame:    
    return df_major_clients.join(df_villes, on="zip", how="inner").select("name", "age", "zip", "city")

# Fonction pour ajouter la colonne "departement" aux informations des clients
def add_departement_column(df: DataFrame) -> DataFrame:
    df = df.withColumn("departement",
                   when((col("zip") >= "20000") & (col("zip") <= "20190"), "2A")
                   .when((col("zip") > "20190") & (col("zip") < "21000"), "2B")
                   .otherwise(expr("substring(zip, 1, 2)"))
    )
    return df.select("name", "age", "zip", "city", "departement")

def main():
    spark = SparkSession.builder \
        .appName("exo2main") \
        .master("local[*]") \
        .getOrCreate()
    
    input_path = "src/resources/exo2/clients_bdd.csv"

    # Lecture des données des clients
    df_clients = read_clients_data(spark, input_path)

    # Filtrage des clients majeurs
    df_major_clients = filter_major_clients(df_clients)

    # Obtention des informations des clients avec les villes
    df_villes = spark.read.option("header", "true").option("delimiter", ",").csv("src/resources/exo2/city_zipcode.csv")
    df_clients_infos = get_clients_with_cities(spark, df_clients, df_villes)

    # Ajout de la colonne "departement"
    df_clients_infos = add_departement_column(df_clients_infos)

    # Sauvegarde les résultats au format Parquet
    output_path = "data/exo2/output"
    df_clients_infos.write.mode("overwrite").format("parquet").option("header", "true").save(output_path)

    # Renommer le fichier de sortie
    liste_fichiers = glob.glob(f"{output_path}/*.parquet")
    if liste_fichiers:
        spark_filename = liste_fichiers[-1]
        os.rename(spark_filename, os.path.join(output_path, "clean.parquet"))
    else:
        print("Aucun fichier Parquet n'a été généré par Spark.")

    # Supprimer les fichiers autres que ".parquet"
    for root, dirs, files in os.walk("data/exo2/output"):
        for file in files:
            if not file.endswith(".parquet"):
                os.remove(os.path.join(root, file))
