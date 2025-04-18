# ETL Pandemia
Ce projet est un ETL réalisé en Python avec Pandas et permettant d'extraire, transformer et charger des données en rapport avec des pandémies dans une base de données MariaDB.

## Installation

1. Cloner le repo Github
```sh
git clone https://github.com/misterwhite44/tester
```

2. Créer un venv
```bash
python3 -m venv venv
```

3. Installer les dépendances
```bash
source venv/bin/activate # Linux
venv\Scripts\activate # Windows

pip install -r requirements.txt
```

## Architecture du projet 
```
└── misterwhite44-tester/
    ├── README.md
    ├── requirements.txt
    ├── .env
    ├── data/
    │   ├── country_wise_latest.csv.csv
    │   ├── owid-monkeypox-data.csv
    │   └── worldometer_coronavirus_daily_data.csv
    ├── db/
    │   └── db_functions.py
    └── scripts/
        ├── import_data.py
        └── main.py
```

## Exemple de .env
```
DB_HOST=your_db_host
DB_USER=your_db_user
DB_PASSWORD=your_user_password
DB_NAME=your_db_name
```