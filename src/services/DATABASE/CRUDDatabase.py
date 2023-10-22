from src.system.secret import DATABASE
from src.system.logging_config import logger
from src.lib.MONGODB.mongodbFunctions import MongoDbFunctions


class CRUDDatabase:
    @staticmethod
    def getSymbols():
        conn = None
        try:
            logger.info("Eliminando datos de la base de datos")
            # OBTENEMOS TODOS LOS ASSETS
            all_assets = []
            # OBTENEMOS TODAS LAS APIs
            api_names = CRUDDatabase.__getApiNames()
            if not api_names[0]:
                logger.error("No se encontraron activos en la base de datos")
                return
            # OBTENEMOS TODOS LOS ASSETS
            for api in api_names[1]:
                config = CRUDDatabase.__getConfig(api)
                all_assets.extend(config["assets"])

            # ELIMINAMOS LOS ASSETS QUE NO ESTAN EN LA LISTA
            conn = MongoDbFunctions(
                DATABASE["host"],
                DATABASE["port"],
                DATABASE["username"],
                DATABASE["password"],
                DATABASE["dbname"],
                "CoreData",
            )

            fields = {
                "symbol": {"$in": all_assets},
            }

            proyeccion = {
                "symbol": 1,
                "_id": 0,
            }  # 1 indica que deseas incluir el campo, 0 indica que no deseas incluirlo

            res = conn.findByMultipleFields(
                fields, custom=True, get_all=True, proyeccion=proyeccion
            )
            symbols_array = [item["symbol"] for item in res]
            unique = list(set(symbols_array))
            return unique
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def getFullSymbolData(symbol):
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
            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            symbol = conn.findByMultipleFields(
                custom=True, fields={"symbol": symbol, "interval": "1day"}
            )
            logger.info("Simbolo obtenida")
            conn.close()
            return symbol
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    # UTIL
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
            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", api_name)
            logger.info("Configuración obtenida")
            conn.close()
            return config_twelve_data_api
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
