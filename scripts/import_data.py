import pandas as pd
from db.db_functions import (
    get_or_insert_continent,
    get_or_insert_country,
    get_or_insert_disease,
    insert_global_data
)
from tqdm import tqdm

# Charger les CSV
df_country_wise = pd.read_csv("data/country_wise_latest.csv.csv")
df_owid_monkeypox = pd.read_csv("data/owid-monkeypox-data.csv")
df_worldometer = pd.read_csv("data/worldometer_coronavirus_daily_data.csv")

# Nettoyer les noms de colonnes pour éviter les erreurs dues aux espaces ou caractères invisibles
df_country_wise.columns = df_country_wise.columns.str.strip()
df_owid_monkeypox.columns = df_owid_monkeypox.columns.str.strip()
df_worldometer.columns = df_worldometer.columns.str.strip()

# Assigner un continent (par exemple, Africa) si tu n'as pas d'information sur le continent dans tes CSV
continent_id = get_or_insert_continent("Africa")  # Remplace par l'ID du continent approprié

# Insertion des données du CSV country_wise_latest.csv.csv
for index, row in tqdm(df_country_wise.iterrows(), total=df_country_wise.shape[0], desc="Insertion des données COVID-19"):
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
for index, row in tqdm(df_owid_monkeypox.iterrows(), total=df_owid_monkeypox.shape[0], desc="Insertion des données Monkeypox"):
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
for index, row in tqdm(df_worldometer.iterrows(), total=df_worldometer.shape[0], desc="Insertion des données Worldometer"):
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
