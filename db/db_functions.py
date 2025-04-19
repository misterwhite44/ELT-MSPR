from dotenv import load_dotenv
import os
import mysql.connector

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def get_db_connection():
    """Établit la connexion à la base de données."""
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")  # Assure-toi que le nom de la base est spécifié dans ton fichier .env
    )
    return connection

def get_or_insert_continent(name):
    """Récupère l'ID du continent ou insère un nouveau continent."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Vérifier si le continent existe déjà
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    # Insérer un nouveau continent
    cursor.execute("INSERT INTO Continent (name) VALUES (%s)", (name,))
    connection.commit()
    
    # Récupérer l'ID du continent nouvellement inséré
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    continent_id = cursor.fetchone()[0]
    connection.close()
    return continent_id

def get_or_insert_country(name, continent_id):
    """Récupère l'ID du pays ou insère un nouveau pays."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Vérifier si le pays existe déjà
    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    # Insérer un nouveau pays
    cursor.execute("INSERT INTO Country (name, continent_id) VALUES (%s, %s)", (name, continent_id))
    connection.commit()
    
    # Récupérer l'ID du pays nouvellement inséré
    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    country_id = cursor.fetchone()[0]
    connection.close()
    return country_id

def get_or_insert_disease(name):
    """Récupère l'ID de la maladie ou insère une nouvelle maladie."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Vérifier si la maladie existe déjà
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    # Insérer une nouvelle maladie
    cursor.execute("INSERT INTO Disease (name) VALUES (%s)", (name,))
    connection.commit()
    
    # Récupérer l'ID de la maladie nouvellement insérée
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    disease_id = cursor.fetchone()[0]
    connection.close()
    return disease_id

def insert_global_data(country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million):
    """Insère les données globales pour une maladie et un pays donné."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Assurer que les champs sont bien définis (éviter l'insertion de `None` pour des colonnes obligatoires)
    cursor.execute("""
        INSERT INTO Global_Data (country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths,
                                 total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        country_id, disease_id, date,
        total_cases if total_cases is not None else None,
        new_cases if new_cases is not None else None,
        total_deaths if total_deaths is not None else None,
        new_deaths if new_deaths is not None else None,
        total_recovered if total_recovered is not None else None,
        new_recovered if new_recovered is not None else None,
        active_cases if active_cases is not None else None,
        serious_critical if serious_critical is not None else None,
        total_tests if total_tests is not None else None,
        tests_per_million if tests_per_million is not None else None
    ))
    
    # Commit des changements dans la base de données
    connection.commit()
    connection.close()

# Exemple d'insertion des données à partir d'un fichier CSV ou d'une autre source
def insert_data_from_csv(df):
    """Fonction pour insérer les données d'un DataFrame dans la base de données."""
    for _, row in df.iterrows():
        continent_name = row['Continent']
        country_name = row['Country/Region']
        disease_name = "COVID-19"  # Exemple pour la maladie (s'adapte à ton cas)
        date = row['Date']
        total_cases = row.get('Total Cases', None)
        new_cases = row.get('New Cases', None)
        total_deaths = row.get('Total Deaths', None)
        new_deaths = row.get('New Deaths', None)
        total_recovered = row.get('Total Recovered', None)
        new_recovered = row.get('New Recovered', None)
        active_cases = row.get('Active Cases', None)
        serious_critical = row.get('Serious Critical', None)
        total_tests = row.get('Total Tests', None)
        tests_per_million = row.get('Tests per Million', None)
        
        # Insérer ou récupérer l'ID du continent
        continent_id = get_or_insert_continent(continent_name)
        
        # Insérer ou récupérer l'ID du pays
        country_id = get_or_insert_country(country_name, continent_id)
        
        # Insérer ou récupérer l'ID de la maladie
        disease_id = get_or_insert_disease(disease_name)
        
        # Insérer les données globales dans la base de données
        insert_global_data(country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million)

