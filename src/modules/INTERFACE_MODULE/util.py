from datetime import datetime, timedelta
import time
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
from requests import Response
from .CleanDatabase import *

# Configure the logger

trading_hours = {
    "start": "15:30",
    "end": "23:00",
}

LIMIT = 2020


def getConfig(api_name):
    conn = None
    try:
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "config",
        )
        logger.debug("Obteniendo configuración de la API Alpha Vantaje")
        config_twelve_data_api = conn.findByField("nombre_api",api_name)
        logger.debug("Configuración obtenida")
        conn.close()
        return config_twelve_data_api
    except Exception as e:
        if conn:
            conn.close()
        logger.error("An error occurred: %s", str(e))
        return (False, e)