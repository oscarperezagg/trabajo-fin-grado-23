from src.system.secret import DATABASE
from src.lib.MONGODB.mongodbFunctions import MongoDbFunctions
from src.system.logging_config import logger


class cleanDatabase:
    @staticmethod
    def purgeData():
        conn = None
        try:
            logger.warning("Eliminando datos de la base de datos")
            # OBTENEMOS TODOS LOS ASSETS
            all_assets = []
            # OBTENEMOS TODAS LAS APIs
            api_names = cleanDatabase.__getApiNames()
            if not api_names[0]:
                logger.error("No se encontraron activos en la base de datos")
                return
            # OBTENEMOS TODOS LOS ASSETS
            for api in api_names[1]:
                config = cleanDatabase.__getConfig(api)
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
                "symbol": {"$nin": all_assets},
            }

            proyeccion = {
                "symbol": 1,
                "_id": 0,
            }  # 1 indica que deseas incluir el campo, 0 indica que no deseas incluirlo

            res = conn.findByMultipleFields(
                fields, custom=True, get_all=True, proyeccion=proyeccion
            )
            symbols_array = [item["symbol"] for item in res]
            if len(symbols_array) == 0:
                logger.warning("No se encontraron activos en la base de datos")
            else:
                logger.critical(f"Eliminando {len(symbols_array)} activos de la base de datos")
                
            for symbol in symbols_array:
                logger.warning(f"Activo: {symbol}")

            # ELIMINAMOS LOS ASSETS QUE ESTÁN EN LA LISTA symbols_array
            fields = {
                "symbol": {"$in": symbols_array},
            }

            conn.deleteByMultipleField(exact_match=False, custom=True, fields=fields)
            logger.warning(
                f"Se eliminaron {len(symbols_array)} activos de la base de datos"
            )

            # Borrando duplicados

            # Realiza la agregación para encontrar documentos duplicados

            
            pipeline = [
                {
                    "$group": {
                        "_id": {"symbol": "$symbol", "interval": "$interval"},
                        "count": {"$sum": 1},
                        "ids": {"$push": "$_id"},
                    }
                },
                {"$match": {"count": {"$gt": 1}, "_id.symbol": {"$ne": "SPX"}}},
            ]
            logger.warning("Eliminando documentos duplicados")
            documentos_duplicados = conn.doAgregate(pipeline)
            logger.warning(f"Documentos detectados: {len(documentos_duplicados)}")
                        # Elimina los documentos duplicados
            for doc in documentos_duplicados:
                doc["ids"].pop(0)  # Mantén uno de los documentos duplicados
                fields = {"_id": {"$in": doc["ids"]}}
                conn.deleteByMultipleField(exact_match=False,custom=True,fields=fields)
                
            logger.warning("Documentos duplicados eliminados")

    
            
            conn.close()
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    @staticmethod
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
