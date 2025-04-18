from dotenv import load_dotenv
import os
import mysql.connector

# Charger les variables d'environnement
load_dotenv()

# Connexion à la base de données
def get_db_connection():
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    return connection

# Fonction pour insérer ou obtenir l'ID du continent
def get_or_insert_continent(name):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO Continent (name) VALUES (%s)", (name,))
    connection.commit()
    cursor.execute("SELECT id FROM Continent WHERE name = %s", (name,))
    return cursor.fetchone()[0]

# Fonction pour insérer ou obtenir l'ID du pays
def get_or_insert_country(name, continent_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO Country (name, continent_id) VALUES (%s, %s)", (name, continent_id))
    connection.commit()
    cursor.execute("SELECT id FROM Country WHERE name = %s", (name,))
    return cursor.fetchone()[0]

# Fonction pour insérer ou obtenir l'ID de la maladie
def get_or_insert_disease(name):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("INSERT INTO Disease (name) VALUES (%s)", (name,))
    connection.commit()
    cursor.execute("SELECT id FROM Disease WHERE name = %s", (name,))
    return cursor.fetchone()[0]

# Fonction pour insérer les données dans Global_Data
def insert_global_data(country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million):
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO Global_Data (country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (country_id, disease_id, date, total_cases, new_cases, total_deaths, new_deaths, total_recovered, new_recovered, active_cases, serious_critical, total_tests, tests_per_million))
    connection.commit()