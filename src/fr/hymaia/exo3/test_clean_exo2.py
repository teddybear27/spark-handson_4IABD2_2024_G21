import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import read_clients_data, filter_major_clients, get_clients_with_cities, add_departement_column

def test_read_clients_data():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadClientsData").getOrCreate()
    
    # Chemin vers un fichier CSV de test pour les clients
    test_clients_csv_path = "tests/data/exo2/test_clients_bdd.csv"
    df = read_clients_data(spark, test_clients_csv_path)
    
    assert df is not None
    assert df.count() > 0  # S'assure que le DataFrame contient des lignes
    assert "name" in df.columns  # Vérifie la présence de la colonne attendue "name"
    
    spark.stop()

def test_filter_major_clients():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestFilterMajorClients").getOrCreate()
    
    # Création d'un DataFrame de test
    data = [("John Doe", 17), ("Jane Doe", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    result_df = filter_major_clients(df)
    
    assert result_df is not None
    assert result_df.count() == 1  # S'attend à ce que seul le client majeur soit retenu
    assert result_df.collect()[0]["age"] >= 18
    
    spark.stop()

def test_get_clients_with_cities():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestGetClientsWithCities").getOrCreate()
    
    # DataFrames de test pour clients et villes
    clients_data = [("John Doe", 25, "75001")]
    cities_data = [("75001", "Paris")]
    df_clients = spark.createDataFrame(clients_data, ["name", "age", "zip"])
    df_cities = spark.createDataFrame(cities_data, ["zip", "city"])
    
    result_df = get_clients_with_cities(spark, df_clients, df_cities)
    
    assert result_df is not None
    assert result_df.count() > 0
    assert "city" in result_df.columns  # Vérifie que la jointure a fonctionné et que 'city' est présent
    
    spark.stop()


def test_add_departement_column():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestAddDepartementColumn").getOrCreate()
    
    # Création d'un DataFrame de test avec des codes postaux
    data = [("John Doe", "age", "75001", "Paris")]
    df = spark.createDataFrame(data, ["name", "age", "zip", "city"])
    
    result_df = add_departement_column(df)
    
    assert result_df is not None
    assert "departement" in result_df.columns  # Vérifie que la colonne 'departement' a été ajoutée
    # Vérifie que le département correspond au préfixe attendu du code postal
    assert result_df.collect()[0]["departement"] == "75"
    
    spark.stop()

###########################

def test_read_clients_data_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestReadClientsDataError").getOrCreate()
    
    # Chemin vers un fichier CSV qui n'existe pas
    invalid_csv_path = "tests/data/exo2/non_existing_file.csv"
    
    # Le test doit lever une exception lors de la lecture du fichier CSV inexistant
    with pytest.raises(Exception):
        read_clients_data(spark, invalid_csv_path)
    
    spark.stop()


def test_filter_major_clients_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestFilterMajorClientsError").getOrCreate()
    
    # Création d'un DataFrame de test vide
    empty_df = spark.createDataFrame([], ["name", "age"])
    
    # Le test doit lever une exception lorsqu'il n'y a aucun client dans le DataFrame
    with pytest.raises(Exception):
        filter_major_clients(empty_df)
    
    spark.stop()


def test_get_clients_with_cities_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestGetClientsWithCitiesError").getOrCreate()
    
    # DataFrames de test vides
    empty_clients_df = spark.createDataFrame([], ["name", "age", "zip"])
    empty_cities_df = spark.createDataFrame([], ["zip", "city"])
    
    # Le test doit lever une exception lorsqu'il n'y a aucune donnée dans les DataFrames
    with pytest.raises(Exception):
        get_clients_with_cities(spark, empty_clients_df, empty_cities_df)
    
    spark.stop()


def test_add_departement_column_error():
    spark = SparkSession.builder.master("local[2]").appName("UnitTestAddDepartementColumnError").getOrCreate()
    
    # Création d'un DataFrame de test avec un code postal invalide
    invalid_zip_data = [("John Doe", "age", "invalid_zip", "Paris")]
    invalid_zip_df = spark.createDataFrame(invalid_zip_data, ["name", "age", "zip", "city"])
    
    # Le test doit lever une exception lorsqu'un code postal invalide est rencontré
    with pytest.raises(Exception):
        add_departement_column(invalid_zip_df)
    
    spark.stop()

