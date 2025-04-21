from dotenv import load_dotenv
import os
import mysql.connector

#  Chargement des variables d'environnement
load_dotenv()

def get_db_connection():
    """Établit la connexion à la base de données."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def get_or_insert_continent(name):
    """Récupère ou insère un continent."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    result = cursor.fetchone()

    if result:
        conn.close()
        return result[0]

    cursor.execute("INSERT INTO Continent (name) VALUES (%s)", (name,))
    conn.commit()

    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    continent_id = cursor.fetchone()[0]

    conn.close()
    return continent_id

def get_or_insert_country(name, continent_id, iso_code=None):
    """Récupère ou insère un pays (et met à jour iso_code si manquant)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    result = cursor.fetchone()

    if result:
        country_id = result[0]
        if iso_code:
            cursor.execute("SELECT code3 FROM Country WHERE id = %s", (country_id,))
            current_code = cursor.fetchone()[0]
            if not current_code:
                cursor.execute("UPDATE Country SET code3 = %s WHERE id = %s", (iso_code, country_id))
                conn.commit()
        conn.close()
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
    conn.commit()

    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    country_id = cursor.fetchone()[0]

    conn.close()
    return country_id

def get_or_insert_disease(name):
    """Récupère ou insère une maladie."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    result = cursor.fetchone()

    if result:
        conn.close()
        return result[0]

    cursor.execute("INSERT INTO Disease (name) VALUES (%s)", (name,))
    conn.commit()

    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    disease_id = cursor.fetchone()[0]

    conn.close()
    return disease_id

def insert_global_data(
    country_id, disease_id, date, total_cases, new_cases,
    total_deaths, new_deaths, total_recovered, new_recovered,
    active_cases, serious_critical, total_tests, tests_per_million
):
    """Insère des données dans la table Global_Data."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO Global_Data (
            country_id, disease_id, date,
            total_cases, new_cases,
            total_deaths, new_deaths,
            total_recovered, new_recovered,
            active_cases, serious_critical,
            total_tests, tests_per_million
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        country_id, disease_id, date,
        total_cases, new_cases,
        total_deaths, new_deaths,
        total_recovered, new_recovered,
        active_cases, serious_critical,
        total_tests, tests_per_million
    ))

    conn.commit()
    conn.close()
