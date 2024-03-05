import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo1.main import read_csv_data, wordcounts

def test_read_csv_data():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadCSVData").getOrCreate()
    
    # Chemin vers un fichier CSV de test dans tests/data/exo1
    test_csv_path = "tests/data/exo1/test_data.csv"
    df = read_csv_data(spark, test_csv_path)
    
    assert df is not None
    assert df.count() > 0  # Vérifie que le DataFrame contient des lignes
    assert "text" in df.columns  # Vérifie la colonne attendue "text"
    
    spark.stop()


def test_wordcounts():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestWordCounts").getOrCreate()
    
    # Création d'un DataFrame de test
    data = [("Row one with text",), ("Another row, with separated text",)]
    df = spark.createDataFrame(data, ["text"])
    
    result_df = wordcounts(df, "text")
    
    assert result_df is not None
    assert result_df.count() > 0  # Vérifie que le résultat contient des mots comptés
    assert "word" in result_df.columns and "count" in result_df.columns
    
    spark.stop()


#################################
    

def test_read_csv_data_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadCSVDataError").getOrCreate()
    
    # Chemin vers un fichier CSV qui n'existe pas
    invalid_csv_path = "tests/data/exo1/non_existing_file.csv"
    
    # Le test doit lever une exception lors de la lecture du fichier CSV inexistant
    with pytest.raises(Exception):
        read_csv_data(spark, invalid_csv_path)
    
    spark.stop()


def test_wordcounts_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestWordCountsError").getOrCreate()
    
    # Création d'un DataFrame de test vide
    empty_df = spark.createDataFrame([], ["text"])
    
    # Le test doit lever une exception lorsqu'il n'y a aucun texte dans le DataFrame
    with pytest.raises(Exception):
        wordcounts(empty_df, "text")
    
    spark.stop()
