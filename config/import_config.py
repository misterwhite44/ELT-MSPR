import_mappings = [
    {
        "path": "data/country_wise_latest.csv",
        "disease": "COVID-19",
        "country_column": "Country/Region",
        "continent_column": "WHO Region",
        "date_column": None,
        "fields": {
            "total_cases": "Confirmed",
            "new_cases": "New cases",
            "total_deaths": "Deaths",
            "new_deaths": "New deaths",
            "total_recovered": "Recovered",
            "new_recovered": "New recovered",
            "active_cases": "Active"
        }
    },
    {
        "path": "data/worldometer_data.csv.xls",
        "country_column": "Country/Region",
        "continent_column": "Continent",
        "population_column": "Population"
    },
    {
        "path": "data/usa_county_wise.csv",
        "country_column": "Province_State",
        "region_column": "Province_State"
    },
    {
        "path": "data/owid-monkeypox-data.csv",
        "disease": "Monkeypox",
        "country_column": "location",
        "iso_column": "iso_code",
        "date_column": "date",
        "fields": {
            "total_cases": "total_cases",
            "new_cases": "new_cases",
            "total_deaths": "total_deaths",
            "new_deaths": "new_deaths",
            "active_cases": "active_cases",
            "total_tests": "total_tests",
            "tests_per_million": "tests_per_million"
        }
    },
    {
        "path": "data/worldometer_coronavirus_daily_data.csv",
        "disease": "COVID-19",
        "country_column": "country",
        "date_column": "date",
        "fields": {
            "total_cases": "cumulative_total_cases",
            "new_cases": "daily_new_cases",
            "total_deaths": "cumulative_total_deaths",
            "new_deaths": "daily_new_deaths",
            "active_cases": "active_cases"
        }
    },
    
]
