from datetime import datetime, timedelta
import time
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
from requests import Response

# Configure the logger

trading_hours = {
    "start": "15:30",
    "end": "23:00",
}

LIMIT = 2018


class AV_CoreData:
    """
    Esta función implementa la lógica detrás de la descarga los "CoreData"
    del proveedor Alpha Vantaje.

    """

    ######################
    ## DOWNLOAD METHODS ##
    ######################

    @staticmethod
    def downloadAsset():
        """
        Esta función descarga activos de cualquier tipo

        :return: Un diccionario con los datos descargados.
        """
        logger.info("[START] Downloading assets with Alpha Vantaje API")
        try:
            # Obtener todos los registros de descarga del activo seleccionado
            config = AV_CoreData.__getConfig()
            timestamps = config["timestamps"].keys()
            assets = config["assets"]

            # Más lógica de descarga aquí...
            for timestamp in timestamps:
                logger.info("Downloading data for %s", timestamp)
                for asset in assets:
                    logger.info("Downloading %s data", asset)
                    # Añadir check para ver si ya está

                    res = AV_CoreData.__findAsset(asset, timestamp)
                    if res[0]:
                        continue

                    if timestamp in ["1month", "1week", "1day"]:
                        res = AV_CoreData.__downloadNoIntradayDataAsset(
                            asset, timestamp
                        )
                    else:
                        res = AV_CoreData.__downloadIntradayAsset(
                            asset, timestamp, LIMIT
                        )

                    if not res[0] and res[1] == "Llamadas diarias agotadas":
                        return (False, "Llamadas diarias agotadas")

                    if not res[0]:
                        logger.error("An error occurred: %s", str(res[1]))
                        return (False, "")

                    if res[0]:
                        logger.info("Asset downloaded successfully")

        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)
        logger.info("[END] Downloading assets with Alpha Vantaje API")

    def __downloadNoIntradayDataAsset(asset, interval):
        try:
            logger.info(
                "Downloading asset data for %s with interval %s", asset, interval
            )

            # Comprobar si hay llamadas disponibles
            check = AV_CoreData.__anotherCall()
            if not check[0]:
                return check

            params = {
                "symbol": asset,
                "apikey": ALPHA_VANTAGE_API_KEY,
            }

            if interval == "1month":
                response = CoreStockAPIs.time_series_monthly(**params)
            elif interval == "1week":
                response = CoreStockAPIs.time_series_weekly(**params)
            elif interval == "1day":
                response = CoreStockAPIs.time_series_daily(**params)
            else:
                logger.error(
                    "Failed to download data for %s with interval %s", asset, interval
                )
                return (False, "Interval not recognized")

            # Sumar 1 call
            AV_CoreData.__oneMoreCall()

            if not response[0]:
                logger.error(
                    "Failed to download data for %s with interval %s", asset, interval
                )
                return (False, response)

            data = response[1].json()

            data = AV_CoreData.__parseMonthlyData(data, interval)
            if not data[0]:
                return data
            upload = AV_CoreData.__uploadAssetDate(data[1])
            if not upload[0]:
                return upload
            logger.info(
                "Data successfully downloaded for %s with interval %s", asset, interval
            )
            return (True, "")
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __downloadIntradayAsset(asset, interval, limit):
        try:
            logger.info(
                "Downloading asset data for %s with interval %s", asset, interval
            )

            # Creamos el objeto final que vamos a subir
            finalDataSet = {}
           

            params = {
                "symbol": asset,
                "apikey": ALPHA_VANTAGE_API_KEY,
                "interval": interval,
                "month":""
            }

            # Obtenemos el mes actual
            now = datetime.now()
            actual_month = now.month
            actual_year = now.year
            # Lista de años y meses
            years_and_months = []
            # Lista de años desde year hasta LIMIT
            years = list(range(actual_year, limit, -1))
            for year in years:
                first_month = actual_month if year == actual_year else 12
                # Recorremos los meses desde first_month hasta el mes 1
                months = list(range(first_month, 0, -1))
                for month in months:
                    # Formato year-month
                    years_and_months.append(str(year) + "-" + str(month).zfill(2))
                    
            logger.info("Llamadas necesarias: %s", len(years_and_months))
            
            # Obtenemos los datos
            for year_month in years_and_months:
                params["month"] = year_month
                
                # Comprobar si hay llamadas disponibles
                check = AV_CoreData.__anotherCall()
                if not check[0]:
                    return check
                
                response = CoreStockAPIs.time_series_intraday(**params)
                
                # Sumar 1 call
                AV_CoreData.__oneMoreCall()

                # Comprobamos si hay errores
                if not response[0]:
                    logger.error(
                        "Failed to download data for %s with interval %s", asset, interval
                    )
                    return (False, response)

                # Obtenemos los datos
                data = response[1].json()

                # Parseamos los datos
                data = AV_CoreData.__parseMonthlyData(data, interval)
                if not data[0]:
                    return data
                
                if finalDataSet == {}:
                    finalDataSet = data[1]
                else:
                    finalDataSet["data"].extend(data[1]["data"])

            upload = AV_CoreData.__uploadAssetDate(finalDataSet)
            if not upload[0]:
                return upload
            logger.info(
                "Data successfully downloaded for %s with interval %s", asset, interval
            )
            return (True, "")
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    ######################
    ## UPDATING METHODS ##
    ######################

    @staticmethod
    def updateAssets():
        """
        Esta función activos de cualquier tipo

        :return: Un diccionario con los datos descargados.
        """
        logger.info("[START] Updating assets with Alpha Vantaje API")

        # Obtenemos el horario de trading de hoy
        start_datetime, end_datetime = AV_CoreData.__trading_hours()
        now = datetime.now()

        if not (start_datetime <= now <= end_datetime):
            logger.info("No estamos en horario de trading")
            logger.info("[END] Updating assets with Alpha Vantaje API")

            return (False, "No estamos en horario de trading")

        try:
            # Obtener todos los registros de descarga del activo seleccionado
            config = AV_CoreData.__getConfig()
            timestamps = config["timestamps"]
            assets = config["assets"]
            for timestamp in timestamps.keys():
                res = AV_CoreData.__updatable_assets(timestamp, assets)
                if not res[0]:
                    break
                for asset in res[1]:
                    is_there_more_time = AV_CoreData.__is_there_more_time(
                        asset, timestamps
                    )

                    if is_there_more_time[0]:
                        res = AV_CoreData.__UpdateAsset(
                            asset["symbol"], timestamp, asset
                        )

        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)
        logger.info("[END] Updating assets with Alpha Vantaje API")

    def __UpdateAsset(asset, interval, complete_asset):
        try:
            # Obtener la configuración de la API

            # Descargar datos
            finalDataSet = {}
            moreData = True
            earliestTimestamp = None
            start_date = complete_asset["data"][0]["datetime"]

            while moreData:
                # Comprobar si hay llamadas disponibles
                check = AV_CoreData.__anotherCall()
                if not check[0]:
                    return check

                response = AV_CoreData.__update_assetDataRange(
                    asset, interval, start_date, finalDataSet
                )

                if not response[0]:
                    return response

                if isinstance(response[1], Response):
                    return (False, response[1])

                # Sumar 1 call
                AV_CoreData.__oneMoreCall()

                finalDataSet["data"] = response[1]["data"]

                moreData = response[2]
                if moreData and response[1]["data"]:
                    start_date = response[1]["data"][0]["datetime"]

            # Editamos el Asset
            if not finalDataSet["data"]:
                logger.error("No hay datos que actualizar")
                return (False, "No hay datos que actualizar")

            tempData = complete_asset["data"]
            complete_asset["data"] = finalDataSet["data"]
            complete_asset["data"].extend(tempData)
            # Subir datos
            res = AV_CoreData.__updateAssetData(complete_asset)
            if not res[0]:
                return res
            # Fin
            logger.info("%s data downloaded successfully", str(asset))
            return (True, asset)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __update_assetDataRange(asset, interval, start_date=None, parseData={}):
        try:
            logger.info(
                "Downloading asset data for %s with interval %s", asset, interval
            )

            params = {
                "symbol": asset,
                "interval": interval,
                "outputsize": "5000",
                "previous_close": "true",
                "start_date": start_date,
                "apikey": TWELVE_DATA_API_KEY,
            }

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

            moreData = True if len(temporalDataSet["values"]) > 5000 else False

            if parseData == {}:
                parseData["data"] = temporalDataSet["values"][:-1]
            else:
                tempData = parseData["data"]
                parseData["data"] = temporalDataSet["values"][:-1]
                parseData["data"].extend(tempData)

            logger.info(
                "Data successfully downloaded for %s with interval %s", asset, interval
            )
            return (True, parseData, moreData)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __updateAssetData(assetData):
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
            id = assetData["_id"]
            del assetData["_id"]
            conn.updateById(id, dict(assetData))
            logger.info("Asset uploaded successfully to the database")
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    ######################
    ### SUPORT METHODS ###
    ######################

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
            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
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
            config_twelve_data_api = AV_CoreData.__getConfig()

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
                    AV_CoreData.__DailyCallTOZero()
                    AV_CoreData.__minuteCallTOZero()
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
                    AV_CoreData.__minuteCallTOZero()
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
                AV_CoreData.__minuteCallTOZero()
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

    def __is_there_more_time(asset_data, timestamps):
        interval = asset_data["interval"]  # 2023-10-19 13:05:00
        try:
            last_datetime = datetime.strptime(
                asset_data["data"][0]["datetime"], "%Y-%m-%d"
            )
        except Exception as e:
            last_datetime = datetime.strptime(
                asset_data["data"][0]["datetime"], "%Y-%m-%d %H:%M:%S"
            )

        time = timestamps[interval][0]
        unit = timestamps[interval][1]

        fecha_actual = datetime.now()

        # Utiliza la variable 'unit' en lugar de 'days' en timedelta
        if unit == "days":
            logger.info("La unidad es días")
            fecha_despues = last_datetime + timedelta(days=time)
        elif unit == "hours":
            logger.info("La unidad es horas")
            fecha_despues = last_datetime + timedelta(hours=time)
        elif unit == "minutes":
            logger.info("La unidad es minutos")
            fecha_despues = last_datetime + timedelta(minutes=time)
        else:
            # Manejar casos no reconocidos o desconocidos
            logger.error("Unrecognized unit: %s", unit)
            return (False, "Unrecognized unit")

        # Verifica si la fecha un mes después es anterior a la fecha actual
        check = fecha_despues < fecha_actual

        if not check:
            logger.info("Data does not need to be updated")
            return (False, "")

        if interval in ["1month", "1week", "1day"]:
            return (True, asset_data)

        # Comprobamos si esta muy desactualizado y lo actualizamos
        new_actual = fecha_actual - timedelta(days=2)
        if new_actual > fecha_despues:
            logger.info("Data needs to be updated")
            return (True, asset_data)

        # Obtenemos el horario de trading de hoy
        start_datetime, end_datetime = AV_CoreData.__trading_hours()

        if start_datetime < fecha_despues < end_datetime:
            logger.info("Data needs to be updated")
            return (True, asset_data)

        while True:
            if unit == "hours":
                logger.info("La unidad es horas")
                fecha_despues = fecha_despues + timedelta(hours=time)
            elif unit == "minutes":
                logger.info("La unidad es minutos")
                fecha_despues = fecha_despues + timedelta(minutes=time)
            else:
                # Manejar casos no reconocidos o desconocidos
                logger.error("Unrecognized unit: %s", unit)
                return (False, "Unrecognized unit")

            logger.info("Comprobamos si nos hemos pasado de la fecha actual")
            if fecha_despues > fecha_actual:
                logger.info("Data does not need to be updated")
                return (False, "")

            logger.info("Comprobamos si la fecha está dentro del horario de trading")
            if start_datetime < fecha_despues < end_datetime:
                logger.info("Data needs to be updated")
                return (True, asset_data)

    def __updatable_assets(interval, assets):
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
            fields = {
                "$and": [
                    {"symbol": {"$in": assets}},
                    {"interval": interval},
                ]
            }
            updatable_stocks = conn.findByMultipleFields(
                fields=fields, custom=True, get_all=True
            )
            conn.close()
            if updatable_stocks:
                logger.info("Configuración obtenida")
                return (True, updatable_stocks)
            else:
                logger.error("No hay activos que actualizar")
                return (False, "No hay activos que actualizar")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __trading_hours():
        # Comprobamos si la fecha está dentro del horario de trading
        now = datetime.now()
        # Obtén las horas de inicio y finalización desde tu diccionario
        start_time = datetime.strptime(trading_hours["start"], "%H:%M")
        end_time = datetime.strptime(trading_hours["end"], "%H:%M")

        # Combina la fecha actual con las horas de inicio y finalización
        start_datetime = datetime(
            now.year, now.month, now.day, start_time.hour, start_time.minute
        )
        end_datetime = datetime(
            now.year, now.month, now.day, end_time.hour, end_time.minute
        )

        return start_datetime, end_datetime

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
            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
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
            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
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

            logger.info("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
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

    def __parseMonthlyData(data, interval):
        start_time = time.time()
        finalDataSet = {}
        finalDataSet["symbol"] = data["Meta Data"]["2. Symbol"]
        finalDataSet["interval"] = interval
        finalDataSet["data"] = []
        keys = list(data.keys())
        data_key = None
        for key in keys:
            if key != "Meta Data":
                data_key = key
        if not data_key:
            return (False, "No data key")
        for key, value in data[data_key].items():
            temp = {}
            temp["datetime"] = key
            temp["open"] = value["1. open"]
            temp["high"] = value["2. high"]
            temp["low"] = value["3. low"]
            temp["close"] = value["4. close"]
            temp["volume"] = value["5. volume"]
            finalDataSet["data"].append(temp)
        elapsed_time = time.time() - start_time
        formatted_time = AV_CoreData.format_time(elapsed_time)
        logger.info("Tiempo de parseo: %s", formatted_time)
        return (True, finalDataSet)

    def format_time(seconds):
        if seconds >= 60:
            # Formatear a minutos con dos decimales
            return "{:.2f} minutos".format(seconds / 60)
        else:
            # Formatear a segundos con dos decimales
            return "{:.2f} segundos".format(seconds)

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
            return (True, "Upload successful")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)
