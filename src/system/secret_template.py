
"""
Este documento es un ejemplo de como debería ser el documento real de secret.py. 
La única diferencia entre el original y este es que este no contiene información sensible.
Los objetos COLLECTIONS y TEST_DOCS son los originales.
"""


ALPHA_VANTAGE_API_KEY = "some_key"

TWELVE_DATA_API_KEY = "some_key"

# This needs to go inside the database
DATABASE = {
    "host": "127.0.0.1",
    "port": "27017",
    "username": None,
    "password": None,
    "dbname": "some_name",
}

COLLECTIONS = [
    "Collection",
    "config",
    "CompanyOverview",
    "IncomeStatement",
    "BalanceSheet",
    "CashFlow",
    "Earnings",
]

TEST_DOCS = {
    "config": {
        "nombre_api": "test_api",
        "fecha_modificacion": {"$date": "2023-11-12T12:45:29.106Z"},
        "llamadas_actuales_diarias": 0,
        "llamadas_actuales_por_minuto": 0,
        "max_llamadas_diarias": 20000,
        "max_llamadas_por_minuto": 10,
        "timestamps": {
            "1month": [30, "days"],
            "1week": [7, "days"],
            "1day": [1, "days"],
            "4h": [4, "hours"],
            "2h": [2, "hours"],
            "1h": [1, "hours"],
            "45min": [45, "minutes"],
            "30min": [30, "minutes"],
            "15min": [15, "minutes"],
            "5min": [5, "minutes"],
        },
        "assets": ["ASSET1", "ASSET2", "ASSET3"],
    }
}
