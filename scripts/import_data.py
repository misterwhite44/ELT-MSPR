import os
import sys
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # Dossier racine
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config')))  # Dossier config
print("Current sys.path:", sys.path)

# Imports internes
from db.db_functions import (
    get_or_insert_continent,
    get_or_insert_country,
    get_or_insert_disease,
    insert_global_data,
    update_population,
    get_or_insert_region
)

from import_config import import_mappings 

load_dotenv()

def clean_columns(df):
    """Supprime les espaces autour des noms de colonnes."""
    df.columns = df.columns.str.strip()
    return df

def convert_to_date(value):
    """Convertit une valeur en date, ou retourne None."""
    try:
        return pd.to_datetime(value, errors='coerce')
    except:
        return None

def process_file(config):
    path = config["path"]
    disease_name = config.get("disease")
    date_column = config.get("date_column")
    continent_column = config.get("continent_column")
    country_column = config["country_column"]
    iso_column = config.get("iso_column")
    fields = config.get("fields", {})

    print(f"\n📄 Traitement du fichier : {path}")
    df = pd.read_csv(path)
    df = clean_columns(df)

    if date_column:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

    for _, row in tqdm(df.iterrows(), total=len(df), desc=disease_name or os.path.basename(path)):
        try:
            continent_name = row.get(continent_column, "Unknown") if continent_column else "Unknown"
            continent_id = get_or_insert_continent(continent_name)
            
            country_name = row[country_column]
            iso_code = row.get(iso_column) if iso_column else None
            country_id = get_or_insert_country(country_name, continent_id, iso_code)

            if "region_column" in config:
                region_name = row.get(config["region_column"], "Unknown")
                get_or_insert_region(region_name, country_id)

            if "population_column" in config:
                population = row.get(config["population_column"])
                update_population(country_id, population)
                continue  

            if disease_name:
                disease_id = get_or_insert_disease(disease_name)
                insert_global_data(
                    country_id,
                    disease_id,
                    row.get(date_column) if date_column else None,
                    row.get(fields.get("total_cases")),
                    row.get(fields.get("new_cases")),
                    row.get(fields.get("total_deaths")),
                    row.get(fields.get("new_deaths")),
                    row.get(fields.get("total_recovered")),
                    row.get(fields.get("new_recovered")),
                    row.get(fields.get("active_cases")),
                    row.get(fields.get("serious_critical")),
                    row.get(fields.get("total_tests")),
                    row.get(fields.get("tests_per_million"))
                )
        except Exception as e:
            print(f" Erreur pour {row.get(country_column)} : {e}")

def main():
    for config in import_mappings:
        process_file(config)

    print("\n Données insérées avec succès !")

if __name__ == "__main__":
    main()
