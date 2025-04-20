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
        database=os.getenv("DB_NAME")
    )
    return connection

def get_or_insert_continent(name):
    """Récupère l'ID du continent ou insère un nouveau continent."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    cursor.execute("INSERT INTO Continent (name) VALUES (%s)", (name,))
    connection.commit()
    
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    continent_id = cursor.fetchone()[0]
    connection.close()
    return continent_id

def get_or_insert_country(name, continent_id, iso_code=None):
    """Récupère l'ID du pays ou insère un nouveau pays (avec iso_code si fourni)."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        country_id = result[0]
        # Met à jour le code3 s'il n'est pas défini et qu'on a un iso_code
        if iso_code:
            cursor.execute("SELECT code3 FROM Country WHERE id = %s", (country_id,))
            current_code = cursor.fetchone()[0]
            if not current_code:
                cursor.execute("UPDATE Country SET code3 = %s WHERE id = %s", (iso_code, country_id))
                connection.commit()
        connection.close()
        return country_id

    # Nouvelle insertion avec iso_code si fourni
    if iso_code:
        cursor.execute(
            "INSERT INTO Country (name, continent_id, code3) VALUES (%s, %s, %s)",
            (name, continent_id, iso_code)
        )
    else:
        cursor.execute(
            "INSERT INTO Country (name, continent_id) VALUES (%s, %s)",
            (name, continent_id)
        )
    connection.commit()

    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    country_id = cursor.fetchone()[0]
    connection.close()
    return country_id

def get_or_insert_disease(name):
    """Récupère l'ID de la maladie ou insère une nouvelle maladie."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    cursor.execute("INSERT INTO Disease (name) VALUES (%s)", (name,))
    connection.commit()
    
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    disease_id = cursor.fetchone()[0]
    connection.close()
    return disease_id

def insert_global_data(country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million):
    """Insère les données globales pour une maladie et un pays donné."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
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
    
    connection.commit()
    connection.close()

def insert_data_from_csv(df):
    """Insère les données d'un DataFrame dans la base de données."""
    for _, row in df.iterrows():
        continent_name = row['Continent']
        country_name = row['Country/Region']
        iso_code = row.get('iso_code', None)  # Nouveau champ
        disease_name = "COVID-19"
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
        
        continent_id = get_or_insert_continent(continent_name)
        country_id = get_or_insert_country(country_name, continent_id, iso_code)
        disease_id = get_or_insert_disease(disease_name)
        
        insert_global_data(
            country_id, disease_id, date,
            total_cases, new_cases, total_deaths, new_deaths,
            total_recovered, new_recovered, active_cases,
            serious_critical, total_tests, tests_per_million
        )
