import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

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

# Charger les CSV
df_country_wise = pd.read_csv("data/country_wise_latest.csv.csv")
df_owid_monkeypox = pd.read_csv("data/owid-monkeypox-data.csv")
df_worldometer = pd.read_csv("data/worldometer_coronavirus_daily_data.csv")

# Nettoyer les noms de colonnes pour éviter les erreurs dues aux espaces ou caractères invisibles
df_country_wise.columns = df_country_wise.columns.str.strip()
df_owid_monkeypox.columns = df_owid_monkeypox.columns.str.strip()
df_worldometer.columns = df_worldometer.columns.str.strip()

# Afficher les colonnes pour chaque fichier CSV pour le débogage
print("Colonnes de 'country_wise_latest.csv.csv':", df_country_wise.columns)
print("Colonnes de 'owid-monkeypox-data.csv':", df_owid_monkeypox.columns)
print("Colonnes de 'worldometer_coronavirus_daily_data.csv':", df_worldometer.columns)

# Assigner un continent (par exemple, Africa) si tu n'as pas d'information sur le continent dans tes CSV
continent_id = get_or_insert_continent("Africa")  # Remplace par l'ID du continent approprié

# Insertion des données du CSV country_wise_latest.csv.csv
for index, row in df_country_wise.iterrows():
    try:
        country_id = get_or_insert_country(row["Country/Region"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, row.get("date", None),
            row.get("Confirmed", None), row.get("New cases", None),
            row.get("Deaths", None), row.get("New deaths", None),
            row.get("Recovered", None), row.get("New recovered", None),
            row.get("Active", None), None, None, None
        )
    except KeyError as e:
        print(f"Erreur lors de l'insertion pour {row['Country/Region']} : {e}")

# Insertion des données du CSV owid-monkeypox-data.csv
for index, row in df_owid_monkeypox.iterrows():
    try:
        country_id = get_or_insert_country(row["location"], continent_id)
        disease_id = get_or_insert_disease("Monkeypox")
        insert_global_data(
            country_id, disease_id, row.get("date", None),
            row.get("total_cases", None), row.get("new_cases", None),
            row.get("total_deaths", None), row.get("new_deaths", None),
            row.get("total_recovered", None), row.get("new_recovered", None),
            row.get("active_cases", None), None, row.get("total_tests", None), row.get("tests_per_million", None)
        )
    except KeyError as e:
        print(f"Erreur lors de l'insertion pour {row['location']} : {e}")

# Insertion des données du CSV worldometer_coronavirus_daily_data.csv
for index, row in df_worldometer.iterrows():
    try:
        country_id = get_or_insert_country(row["country"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, row.get("date", None),
            row.get("cumulative_total_cases", None), row.get("daily_new_cases", None),
            row.get("cumulative_total_deaths", None), row.get("daily_new_deaths", None),
            None, None, row.get("active_cases", None), None, None, None
        )
    except KeyError as e:
        print(f"Erreur lors de l'insertion pour {row['country']} : {e}")

print("Données insérées avec succès !")
