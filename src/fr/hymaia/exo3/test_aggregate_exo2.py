import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import read_parquet_data, process_and_sort_data

def test_read_parquet_data():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadParquetData").getOrCreate()
    
    # Chemin vers un fichier Parquet de test
    test_parquet_path = "tests/data/exo2/test_clean.parquet"
    df = read_parquet_data(spark, test_parquet_path)
    
    assert df is not None
    assert df.count() > 0  # S'assure que le DataFrame contient des lignes
    assert "age" in df.columns  # Vérifie la présence de la colonne attendue "age"
    
    spark.stop()

def test_process_and_sort_data():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestProcessAndSortData").getOrCreate()
    
    # Création d'un DataFrame de test
    data = [("Dept1", "John Doe"), ("Dept2", "Jane Doe")]
    df = spark.createDataFrame(data, ["departement", "name"])
    
    result_df = process_and_sort_data(df)
    
    assert result_df is not None
    assert result_df.count() > 0
    assert result_df.columns == ["departement", "nb_people"]
    
    spark.stop()


############################
    

def test_read_parquet_data_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadParquetDataError").getOrCreate()
    
    # Chemin vers un fichier Parquet qui n'existe pas
    invalid_parquet_path = "tests/data/exo2/non_existing_file.parquet"
    
    # Le test doit lever une exception lors de la lecture du fichier Parquet inexistant
    with pytest.raises(Exception):
        read_parquet_data(spark, invalid_parquet_path)
    
    spark.stop()


def test_process_and_sort_data_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestProcessAndSortDataError").getOrCreate()
    
    # Création d'un DataFrame de test vide
    empty_df = spark.createDataFrame([], ["departement", "name"])
    
    # Le test doit lever une exception lorsqu'il n'y a aucune donnée dans le DataFrame
    with pytest.raises(Exception):
        process_and_sort_data(empty_df)
    
    spark.stop()

