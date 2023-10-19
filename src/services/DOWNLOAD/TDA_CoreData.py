from datetime import datetime, timedelta
import time
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
from requests import Response

# Configure the logger


class TDA_CoreData:
    """
    Esta función implementa la lógica detrás de la descarga los "CoreData"
    del proveedor Twelve Data.

    """

    @staticmethod
    def downloadAsset():
        """
        Esta función descarga activos de cualquier tipo

        :return: Un diccionario con los datos descargados.
        """
        conn = None
        try:
            # Obtener todos los registros de descarga del activo seleccionado
            conn = MongoDbFunctions(
                DATABASE["host"],
                DATABASE["port"],
                DATABASE["username"],
                DATABASE["password"],
                DATABASE["dbname"],
                "DownloadRegistry",
            )

            logger.info(f"Descargando registros")
            DownloadRegistries = conn.findAll(sort=True, sortField="priority")

            # Más lógica de descarga aquí...
            for registry in DownloadRegistries:
                id = registry.get("_id")
                timespan = registry.get("timespan")
                if timespan == "1min":
                    logger.info(
                        f"La funcionalidad no se ha implentado para timespan {timespan}"
                    )
                    continue
                logger.info(f"Descargando registros con timespanp {timespan}")

                for asset in registry["descargas_pendientes"]:
                    # Añadir check para ver si ya está
                    res = TDA_CoreData.__findAsset(asset, timespan)
                    if res[0]:
                        TDA_CoreData.__eliminateFromRegistry(id, asset)
                        continue
                    res = TDA_CoreData.__downloadAsset(id, asset, timespan)
                    if not res[0] and res[1] == "Llamadas diarias agotadas":
                        return (False, "Llamadas diarias agotadas")

                    invalid_item = False
                    try:
                        temporal_res = res[1].json()
                        invalid_item = (
                            f"**symbol** not found: {asset}. Please specify it correctly according to API Documentation."
                            == temporal_res.get("message")
                        )
                    except Exception as e:
                        pass
                    if res[0] or invalid_item:
                        TDA_CoreData.__eliminateFromRegistry(id, asset)
                if not registry["descargas_pendientes"]:
                    logger.info(f"No hay registros de con timespan {timespan}")
                logger.info(f"Descargados todos los registros con timespan {timespan}")

            conn.close()
        except Exception as e:
            if conn:
                conn.close()

            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __downloadAsset(id, asset, interval):
        conn = None
        try:
            if interval == "60min":
                interval = "1h"
            # Obtener la configuración de la API

            # Descargar datos
            finalDataSet = {}
            moreData = True
            earliestTimestamp = None
            end_date = None

            while moreData:
                # Comprobar si hay llamadas disponibles
                check = TDA_CoreData.__anotherCall()
                if not check[0]:
                    return check

                response = TDA_CoreData.__assetDataRange(
                    asset, interval, end_date, finalDataSet
                )
                if not response[0]:
                    return response

                if isinstance(response[1], Response):
                    return (False, response[1])

                # Sumar 1 call
                TDA_CoreData.__oneMoreCall()

                finalDataSet = response[1]

                if not earliestTimestamp:
                    check = TDA_CoreData.__anotherCall()
                    if not check[0]:
                        return check

                    res = TDA_CoreData.__earliestTimestamp(
                        asset, interval, finalDataSet["mic_code"]
                    )

                    # Sumar 1 call
                    TDA_CoreData.__oneMoreCall()

                    if not res[0]:
                        return res

                    earliestTimestamp = res[1]["datetime"]
                    logger.info("Earliest timestamp: %s", earliestTimestamp)

                logger.info(
                    "Last downloaded timestmp: %s", finalDataSet["data"][-1]["datetime"]
                )
                if finalDataSet["data"][-1]["datetime"] == earliestTimestamp:
                    moreData = False
                else:
                    end_date = finalDataSet["data"][-1]["datetime"]
            # Subir datos
            res = TDA_CoreData.__uploadAssetDate(finalDataSet)
            if not res[0]:
                return res
            # Fin
            logger.info("%s data downloaded successfully", str(asset))
            return (True, asset)
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __getConfig():
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
            logger.info("Obteniendo configuración de la API Twelve Data")
            config_twelve_data_api = conn.findByField("nombre_api", "twelvedataapi")
            logger.info("Configuración obtenida")
            conn.close()
            return config_twelve_data_api
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __anotherCall():
        conn = None
        try:
            config_twelve_data_api = TDA_CoreData.__getConfig()

            # Comprobamos si el tiempo de modificación es de hace un año
            last_modification_date = config_twelve_data_api.get("fecha_modificacion")
            if last_modification_date:
                current_date = datetime.now()

                # Calcula la diferencia de tiempo entre la fecha de modificación y la fecha actual
                time_difference = current_date - last_modification_date

                # Define la duración mínima requerida (en este caso, 1 día)
                min_duration = timedelta(days=1)

                # Comprueba si la diferencia de tiempo es mayor que la duración mínima
                if time_difference > min_duration:
                    logger.info("La fecha de modificación es de hace un día.")
                    TDA_CoreData.__DailyCallTOZero()
                    TDA_CoreData.__minuteCallTOZero()
                    return (True, "")

            # Verificamos si la fecha de modificación es de hace más de un minuto y medio
            if last_modification_date:
                # Obtiene la fecha y hora actual
                current_datetime = datetime.now()
                # Calcula la diferencia de tiempo entre la fecha de modificación y la fecha actual
                time_difference = current_datetime - last_modification_date
                # Define la duración máxima permitida (en este caso, 2 minutos)
                max_duration = timedelta(minutes=2)

                if time_difference > max_duration:
                    logger.info(
                        "La fecha de modificación es de hace más de un minuto y medio."
                    )
                    TDA_CoreData.__minuteCallTOZero()
                    return (True, "")

            # Comprobamos llamadas diarias
            check = (
                config_twelve_data_api["llamadas_actuales_diarias"]
                < config_twelve_data_api["max_llamadas_diarias"] - 20
            )
            if not check:
                logger.info("Llamadas diarias agotadas")
                return (False, "Llamadas diarias agotadas")
            # Comprobamos llamadas por minuto
            check = (
                config_twelve_data_api["llamadas_actuales_por_minuto"]
                < config_twelve_data_api["max_llamadas_por_minuto"] - 2
            )
            if not check:
                TDA_CoreData.__minuteCallTOZero()
                logger.info("Esperando 60 segundos...")
                time.sleep(80)
            return (True, "")
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __earliestTimestamp(asset, interval, mic_code):
        try:
            date = ReferenceData.earliest_timestamp(
                symbol=asset,
                interval=interval,
                mic_code=mic_code,
                apikey=TWELVE_DATA_API_KEY,
            )

            if not date[0]:
                return (False, date)

            date = date[1].json()
            if date.get("status") == "error":
                return (False, date)
            logger.info(
                "Earliest timestamp for %s with interval %s is %s",
                asset,
                interval,
                date["datetime"],
            )
            return (True, date)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __assetDataRange(asset, interval, end_date=None, parseData={}):
        try:
            logger.info(
                "Downloading asset data for %s with interval %s", asset, interval
            )

            params = {
                "symbol": asset,
                "interval": interval,
                "outputsize": "5000",
                "previous_close": "true",
                "apikey": TWELVE_DATA_API_KEY,
            }

            if end_date:
                params["end_date"] = end_date

            response = CoreData.time_series_intraday(**params)

            if not response[0]:
                logger.error(
                    "Failed to download data for %s with interval %s", asset, interval
                )
                return (False, response)

            temporalDataSet = response[1].json()

            if temporalDataSet["status"] == "error":
                logger.error(
                    "Received an error response: %s",
                    temporalDataSet.get("message", "Unknown error"),
                )
                return response

            if parseData == {}:
                parseData.update(temporalDataSet["meta"])
                parseData["data"] = temporalDataSet["values"]
            else:
                parseData["data"].extend(temporalDataSet["values"])

            logger.info(
                "Data successfully downloaded for %s with interval %s", asset, interval
            )
            return (True, parseData)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __oneMoreCall():
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

            logger.info("Obteniendo configuración de la API Twelve Data")
            config_twelve_data_api = conn.findByField("nombre_api", "twelvedataapi")
            logger.info("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_diarias"] += 1
            config_twelve_data_api["llamadas_actuales_por_minuto"] += 1
            config_twelve_data_api["fecha_modificacion"] = datetime.now()

            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)

            logger.info("Registry updated successfully")
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __uploadAssetDate(assetData):
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
            logger.info("Uploading data for %s", assetData["symbol"])
            assetData["last_modified"] = datetime.now()

            conn.insert(dict(assetData))
            logger.info("Asset uploaded successfully to the database")
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __eliminateFromRegistry(id, asset):
        conn = None
        try:
            conn = MongoDbFunctions(
                DATABASE["host"],
                DATABASE["port"],
                DATABASE["username"],
                DATABASE["password"],
                DATABASE["dbname"],
                "DownloadRegistry",
            )
            registry = conn.findById(id)
            logger.info("Deleting %s from %s registry", asset, registry["timespan"])

            registry["descargas_pendientes"].remove(asset)
            conn.updateById(id, registry)

            logger.info("Delete successfully")
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __findAsset(asset, interval):
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
            logger.info("Finding %s data for %s", interval, asset)
            fields = {"symbol": asset, "interval": interval}
            res = conn.findByMultipleFields(fields)
            conn.close()
            if res:
                logger.info("Asset already in the database")
                return (True, "")
            return (False, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __minuteCallTOZero():
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
            logger.info("Obteniendo configuración de la API Twelve Data")
            config_twelve_data_api = conn.findByField("nombre_api", "twelvedataapi")
            logger.info("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_por_minuto"] = 0
            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)
            logger.info("Configuración actualizada")
            conn.close()
            logger.info("Llamadas por minuto zeorizadas")
            return (False, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __DailyCallTOZero():
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
            logger.info("Obteniendo configuración de la API Twelve Data")
            config_twelve_data_api = conn.findByField("nombre_api", "twelvedataapi")
            logger.info("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_diarias"] = 0
            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)
            logger.info("Llamadas diarias zeorizadas")
            conn.close()
            return (False, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)
