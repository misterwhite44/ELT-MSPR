import pandas as pd
import os

def lire_csv_pandas(nom_fichier):
    chemin_fichier = os.path.join("data", nom_fichier)

    try:
        df = pd.read_csv(chemin_fichier)
        print(df.head())  # Affiche les 5 premières lignes
    except FileNotFoundError:
        print(f"Le fichier '{nom_fichier}' n'a pas été trouvé dans le dossier 'data'.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")

if __name__ == "__main__":
    lire_csv_pandas("owid-monkeypox-data.csv")
