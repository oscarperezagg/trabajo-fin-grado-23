from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
import logging


class Stats:
    @staticmethod
    def get_stats():

        logger.setLevel(logging.CRITICAL)
        api_names = Stats.__getApiNames()
        if not api_names[0]:
            logger.error("No se encontraron activos en la base de datos")
            return
        stats = {}
        for api in api_names[1]:
            config = Stats.__getConfig(api)
            tempStats = []
            total_assets = len(config["assets"])
            for interval in config["timestamps"]:
                res = Stats.__getDownloadedStocks(api, interval, config["assets"])
                if not res[0]:
                    logger.error("No se encontraron activos en la base de datos")
                    return
                already_downloaded = len(res[1])

                tempStats.append(
                    f"|   Intervalo {interval} -- Descargados: {already_downloaded} / {total_assets} ({round(already_downloaded*100/total_assets, 2)}%)"
                )
            stats[api] = tempStats
        logger.setLevel(logging.DEBUG)
        # Hacer clear de la consola
        print("\033c")
        print("")
        logger.debug("[INICO] - ESTADISTICAS\n")
        for api in api_names[1]:
            logger.debug("ESTADISTICAS PARA LAS APIS: %s", api)
            for stat in stats[api]:
                logger.debug(stat)
            print("")
        logger.debug("[FIN] - ESTADISTICAS\n")
        logger.setLevel(logging.DEBUG)


    def __getDownloadedStocks(api, interval, assets):
        conn = None
        try:
            conn = MongoDbFunctions(
                DATABASE["host"],
                DATABASE["port"],
                DATABASE["username"],
                DATABASE["password"],
                DATABASE["dbname"],
                "CoreData",
            )
            # Realizar la consulta y proyectar solo el campo "symbol"

            fields = {
                "interval": interval,
            }
            if api == "alphavantage":
                fields["symbol"] = {"$ne": "SPX"}
            else:
                fields["symbol"] = "SPX"

            proyeccion = {
                "symbol": 1,
                "_id": 0,
            }  # 1 indica que deseas incluir el campo, 0 indica que no deseas incluirlo

            res = conn.findByMultipleFields(
                fields, get_all=True, custom=True, proyeccion=proyeccion
            )

            symbols_array = [item["symbol"] for item in res]

            conn.close()

            return (True, symbols_array)
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __getApiNames():
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
            # Realizar la consulta y proyectar solo el campo "symbol"
            fields = {}

            proyeccion = {
                "nombre_api": 1,
                "_id": 0,
            }  # 1 indica que deseas incluir el campo, 0 indica que no deseas incluirlo

            res = conn.findByMultipleFields(
                fields, get_all=True, custom=True, proyeccion=proyeccion
            )
            if not res:
                return (False, "No se encontraron activos en la base de datos")

            api_names = [item["nombre_api"] for item in res]

            return (True, api_names)
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __getConfig(api_name):
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
            config_twelve_data_api = conn.findByField("nombre_api", api_name)
            logger.debug("Configuración obtenida")
            conn.close()
            return config_twelve_data_api
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)
