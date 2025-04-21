import os
import mysql.connector
from dotenv import load_dotenv

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
        if iso_code:
            cursor.execute("SELECT code3 FROM Country WHERE id = %s", (country_id,))
            current_code = cursor.fetchone()[0]
            if not current_code:
                cursor.execute("UPDATE Country SET code3 = %s WHERE id = %s", (iso_code, country_id))
                connection.commit()
        connection.close()
        return country_id

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

def get_or_insert_region(name, country_id):
    """Récupère l'ID de la région ou insère une nouvelle région."""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    cursor.execute("SELECT id FROM Region WHERE name = %s AND country_id = %s", (name, country_id))
    result = cursor.fetchone()
    
    if result:
        connection.close()
        return result[0]
    
    cursor.execute("INSERT INTO Region (name, country_id) VALUES (%s, %s)", (name, country_id))
    connection.commit()
    
    cursor.execute("SELECT id FROM Region WHERE name = %s AND country_id = %s", (name, country_id))
    region_id = cursor.fetchone()[0]
    connection.close()
    return region_id

def update_population(country_id, population):
    connection = get_db_connection()
    cursor = connection.cursor()

    # Mettre à jour la population pour le pays donné
    cursor.execute("UPDATE Country SET population = %s WHERE id = %s", (population, country_id))
    connection.commit()
    connection.close()

def get_or_insert_disease(name):
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
