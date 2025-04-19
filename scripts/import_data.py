import os
import sys
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# 🔧 Configuration du chemin du projet pour les imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ✅ Imports internes après configuration du chemin
from db.db_functions import (
    get_or_insert_continent,
    get_or_insert_country,
    get_or_insert_disease,
    insert_global_data
)

# 🔐 Chargement des variables d'environnement
load_dotenv()

# 📁 Chargement des CSV
df_country_wise = pd.read_csv("data/country_wise_latest.csv")
df_owid_monkeypox = pd.read_csv("data/owid-monkeypox-data.csv")
df_worldometer = pd.read_csv("data/worldometer_coronavirus_daily_data.csv")

# 🧹 Nettoyage des colonnes
df_country_wise.columns = df_country_wise.columns.str.strip()
df_owid_monkeypox.columns = df_owid_monkeypox.columns.str.strip()
df_worldometer.columns = df_worldometer.columns.str.strip()

# 🗓️ Conversion de la date Worldometer
df_worldometer['date'] = pd.to_datetime(df_worldometer['date'], errors='coerce')

# 🌍 CONTINENT par défaut (à adapter si besoin)
default_continent = "Africa"
continent_id = get_or_insert_continent(default_continent)

# 🚀 Insertion des données COVID-19
for _, row in tqdm(df_country_wise.iterrows(), total=len(df_country_wise), desc="Insertion des données COVID-19"):
    try:
        country_id = get_or_insert_country(row["Country/Region"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, None,  # Pas de date dans ce CSV
            row.get("Confirmed") if pd.notna(row.get("Confirmed")) else None,
            row.get("New cases") if pd.notna(row.get("New cases")) else None,
            row.get("Deaths") if pd.notna(row.get("Deaths")) else None,
            row.get("New deaths") if pd.notna(row.get("New deaths")) else None,
            row.get("Recovered") if pd.notna(row.get("Recovered")) else None,
            row.get("New recovered") if pd.notna(row.get("New recovered")) else None,
            row.get("Active") if pd.notna(row.get("Active")) else None, 
            None, None, None
        )
    except Exception as e:
        print(f"❌ Erreur pour {row.get('Country/Region')}: {e}")

# 🚀 Insertion des données Monkeypox
for _, row in tqdm(df_owid_monkeypox.iterrows(), total=len(df_owid_monkeypox), desc="Insertion des données Monkeypox"):
    try:
        country_id = get_or_insert_country(row["location"], continent_id)
        disease_id = get_or_insert_disease("Monkeypox")
        insert_global_data(
            country_id, disease_id, row.get("date"),
            row.get("total_cases") if pd.notna(row.get("total_cases")) else None,
            row.get("new_cases") if pd.notna(row.get("new_cases")) else None,
            row.get("total_deaths") if pd.notna(row.get("total_deaths")) else None,
            row.get("new_deaths") if pd.notna(row.get("new_deaths")) else None,
            None, None,
            row.get("active_cases") if pd.notna(row.get("active_cases")) else None,
            None,
            row.get("total_tests") if pd.notna(row.get("total_tests")) else None,
            row.get("tests_per_million") if pd.notna(row.get("tests_per_million")) else None
        )
    except Exception as e:
        print(f"❌ Erreur pour {row.get('location')}: {e}")

# 🚀 Insertion des données Worldometer
for _, row in tqdm(df_worldometer.iterrows(), total=len(df_worldometer), desc="Insertion des données Worldometer"):
    try:
        country_id = get_or_insert_country(row["country"], continent_id)
        disease_id = get_or_insert_disease("COVID-19")
        insert_global_data(
            country_id, disease_id, row.get("date"),
            row.get("cumulative_total_cases") if pd.notna(row.get("cumulative_total_cases")) else None,
            row.get("daily_new_cases") if pd.notna(row.get("daily_new_cases")) else None,
            row.get("cumulative_total_deaths") if pd.notna(row.get("cumulative_total_deaths")) else None,
            row.get("daily_new_deaths") if pd.notna(row.get("daily_new_deaths")) else None,
            None, None,
            row.get("active_cases") if pd.notna(row.get("active_cases")) else None,
            None, None, None
        )
    except Exception as e:
        print(f"❌ Erreur pour {row.get('country')}: {e}")

print("✅ Données insérées avec succès !")