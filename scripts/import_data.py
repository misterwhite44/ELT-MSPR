import os
import sys
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

#  Configuration du chemin du projet pour les imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#  Imports internes après configuration du chemin
from db.db_functions import (
    get_or_insert_continent,
    get_or_insert_country,
    get_or_insert_disease,
    insert_global_data,
    update_population
)

#  Chargement des variables d'environnement
load_dotenv()

#  Chargement des CSV
df_country_wise = pd.read_csv("data/country_wise_latest.csv")
df_owid_monkeypox = pd.read_csv("data/owid-monkeypox-data.csv")
df_worldometer = pd.read_csv("data/worldometer_coronavirus_daily_data.csv")
df_worldometer_data = pd.read_csv("data/worldometer_data.csv.xls")


# 🧹 Nettoyage des colonnes
for df in [df_country_wise, df_owid_monkeypox, df_worldometer, df_worldometer_data]:
    df.columns = df.columns.str.strip()

#  Conversion des dates
df_worldometer['date'] = pd.to_datetime(df_worldometer['date'], errors='coerce')
df_owid_monkeypox['date'] = pd.to_datetime(df_owid_monkeypox['date'], errors='coerce')

#  Insertion des données COVID-19
for _, row in tqdm(df_country_wise.iterrows(), total=len(df_country_wise), desc="COVID-19 (country_wise_latest)"):
    try:
        continent_id = get_or_insert_continent(row.get("WHO Region", "Unknown"))
        country_id = get_or_insert_country(row["Country/Region"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, None,
            row.get("Confirmed"), row.get("New cases"),
            row.get("Deaths"), row.get("New deaths"),
            row.get("Recovered"), row.get("New recovered"),
            row.get("Active"), None, None, None
        )
    except Exception as e:
        print(f"❌ Erreur pour {row.get('Country/Region')}: {e}")

for _, row in tqdm(df_worldometer_data.iterrows(), total=len(df_worldometer_data), desc="Mise à jour des populations"):
    try:
        country_name = row["Country/Region"]
        population = row.get("Population")
        continent_id = get_or_insert_continent(row.get("Continent", "Unknown"))
        country_id = get_or_insert_country(country_name, continent_id)
        
        # Mise à jour de la population
        update_population(country_id, population)
    except Exception as e:
        print(f"❌ Erreur pour {row.get('Country/Region')}: {e}")

for _, row in tqdm(df_owid_monkeypox.iterrows(), total=len(df_owid_monkeypox), desc="Monkeypox (owid)"):
    try:
        continent_id = get_or_insert_continent("Unknown")
        iso_code = row.get("iso_code", None)
        country_id = get_or_insert_country(row["location"], continent_id, iso_code)
        disease_id = get_or_insert_disease("Monkeypox")
        insert_global_data(
            country_id, disease_id, row.get("date"),
            row.get("total_cases"), row.get("new_cases"),
            row.get("total_deaths"), row.get("new_deaths"),
            None, None,
            row.get("active_cases"), None,
            row.get("total_tests"), row.get("tests_per_million")
        )
    except Exception as e:
        print(f"Erreur pour {row.get('location')}: {e}")

#  Insertion des données Worldometer
for _, row in tqdm(df_worldometer.iterrows(), total=len(df_worldometer), desc="COVID-19 (worldometer)"):
    try:
        continent_id = get_or_insert_continent("Unknown")
        country_id = get_or_insert_country(row["country"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, row.get("date"),
            row.get("cumulative_total_cases"), row.get("daily_new_cases"),
            row.get("cumulative_total_deaths"), row.get("daily_new_deaths"),
            None, None,
            row.get("active_cases"), None, None, None
        )
    except Exception as e:
        print(f" Erreur pour {row.get('country')}: {e}")


print("Données insérées avec succès !")
