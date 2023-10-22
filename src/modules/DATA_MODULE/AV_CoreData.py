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

LIMIT = 2020


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
            all_assets = config["assets"]
            deleted_assets = []
            # Más lógica de descarga aquí...
            for timestamp in timestamps:
                logger.info("Downloading data for %s", timestamp)
                # Obtenemos los asset que hay que descargar obteniendo los assets para el internvalo
                res = AV_CoreData.__assestToBeDownloaded(timestamp, all_assets)
                if not res[0]:
                    return res
                assets = res[1]
                total_assets = len(assets)
                # Descargamos los assets
                for index, asset in enumerate(assets):
                    if asset in deleted_assets:
                        continue
                    logger.info("Downloading %s data", asset)
                    logger.info(
                        "Asset %s of %s (%.2f%%)",
                        index + 1,
                        total_assets,
                        (index + 1) * 100 / total_assets,
                    )

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

                    if res[0] and res[1] == "Asset not found on Alpha Vantaje API":
                        deleted_assets.append(res[2])
                        continue

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
                params["outputsize"] = "full"
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

            if data and data.get("Error Message"):
                logger.error("Asset not found on Alpha Vantaje API")
                logger.error(data.get("Error Message"))
                error = data.get("Error Message")
                if error and "Invalid API call" in error:
                    res = AV_CoreData.__deleteAssetFromConfig(asset)
                    if not res[0]:
                        logger.error("An error occurred: Asset no deleted")
                        return res
                    logger.warning("Asset deleted successfully")
                    return (True, "Asset not found on Alpha Vantaje API", asset)
                else:
                    return (False, data.get("Error Message"))

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
                "month": "",
                "outputsize": "full",
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
                        "Failed to download data for %s with interval %s",
                        asset,
                        interval,
                    )
                    return (False, response)

                # Obtenemos los datos
                data = response[1].json()

                # Comprobamos si hay errores
                if data and data.get("Error Message"):
                    logger.error("Asset not found on Alpha Vantaje API")
                    logger.error(data.get("Error Message"))
                    error = data.get("Error Message")
                    if error and "Invalid API call" in error:
                        res = AV_CoreData.__deleteAssetFromConfig(asset)
                        if not res[0]:
                            logger.error("An error occurred: Asset no deleted")
                            return res
                        logger.warning("Asset deleted successfully")
                        return (True, "Asset not found on Alpha Vantaje API", asset)
                    else:
                        return (False, data.get("Error Message"))

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

            for timestamp in timestamps.keys():
                res = AV_CoreData.__assestToBeUpdated(timestamp)
                if not res[0]:
                    return res
                assets = res[1]
                total_assets = len(assets)

                for index, asset in enumerate(assets):
                    logger.info(
                        "Asset %s of %s (%.2f%%)",
                        index + 1,
                        total_assets,
                        (index + 1) * 100 / total_assets,
                    )
                    is_there_more_time = AV_CoreData.__is_there_more_time(
                        asset, timestamps
                    )

                    if is_there_more_time[0]:
                        logger.warning(f"Updating {asset['symbol']} for {timestamp}...")
                        complete_asset = AV_CoreData.__getAsset(asset)
                        if timestamp in ["1month", "1week", "1day"]:
                            # ELIMINAMOS LOS DATOS DEL ACTIVO
                            res = AV_CoreData.__deleteAsset(complete_asset)
                            res = AV_CoreData.__downloadNoIntradayDataAsset(
                                complete_asset["symbol"], timestamp
                            )
                        else:
                            res = AV_CoreData.__updateAsset(
                                complete_asset["symbol"], timestamp, complete_asset
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
        logger.info("[END] Updating assets with Alpha Vantaje API")

    def __updateAsset(asset, interval, complete_asset):
        try:
            # Obtener la configuración de la API

            try:
                start_date = datetime.strptime(
                    complete_asset["data"][0]["datetime"], "%Y-%m-%d"
                )
            except Exception as e:
                start_date = datetime.strptime(
                    complete_asset["data"][0]["datetime"], "%Y-%m-%d %H:%M:%S"
                )
            # Completamos el mes actual
            now = datetime.now()
            actual_month = now.month
            actual_year = now.year
            years = list(range(start_date.year, actual_year + 1, 1))
            years_and_months = []
            total_years = len(years)

            if total_years == 1:
                months = list(range(start_date.month, actual_month + 1, 1))
                for month in months:
                    # Formato year-month
                    years_and_months.append(str(years[0]) + "-" + str(month).zfill(2))
            else:
                for index, year in enumerate(years):
                    if index == 0:
                        months = list(range(start_date.month, 13, 1))
                    elif index + 1 == total_years:
                        months = list(range(1, actual_month + 1, 1))
                    else:
                        months = list(range(1, 13, 1))
                    for month in months:
                        # Formato year-month
                        years_and_months.append(str(year) + "-" + str(month).zfill(2))

            # Obtenemos los datos
            finalDataSet = {}

            params = {
                "symbol": asset,
                "apikey": ALPHA_VANTAGE_API_KEY,
                "interval": interval,
                "month": "",
                "outputsize": "full",
            }

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
                        "Failed to download data for %s with interval %s",
                        asset,
                        interval,
                    )
                    return (False, response)

                # Obtenemos los datos
                data = response[1].json()

                # Comprobamos si hay errores
                if data and data.get("Error Message"):
                    logger.error("Asset not found on Alpha Vantaje API")
                    logger.error(data.get("Error Message"))
                    error = data.get("Error Message")
                    if error and "Invalid API call" in error:
                        res = AV_CoreData.__deleteAssetFromConfig(asset)
                        if not res[0]:
                            logger.error("An error occurred: Asset no deleted")
                            return res
                        logger.warning("Asset deleted successfully")
                        return (True, "Asset not found on Alpha Vantaje API", asset)
                    else:
                        return (False, data.get("Error Message"))

                # Parseamos los datos
                data = AV_CoreData.__parseMonthlyData(data, interval)
                if not data[0]:
                    return data

                if finalDataSet == {}:
                    finalDataSet = data[1]
                else:
                    finalDataSet["data"].extend(data[1]["data"])

            # Eliminar los datos del mes start_date.month de complete_asset
            index = 0
            for i in range(len(complete_asset["data"])):
                tempDate = complete_asset["data"][i]["datetime"]
                try:
                    tempDate = datetime.strptime(
                        complete_asset["data"][0]["datetime"], "%Y-%m-%d"
                    )
                except Exception as e:
                    tempDate = datetime.strptime(
                        complete_asset["data"][0]["datetime"], "%Y-%m-%d %H:%M:%S"
                    )

                if tempDate.month != start_date.month:
                    break

                index += 1

            tempData = complete_asset["data"][index:]
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

                # Comprueba si la diferencia de tiempo es mayor que la duración mínima
                if current_date.day > last_modification_date.day:
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
                logger.warning("Llamadas diarias agotadas")
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
        numero_semana_actual = fecha_actual.isocalendar()[1]
        numero_semana_dato = last_datetime.isocalendar()[1]

        if interval == "1month":
            if fecha_actual.year > last_datetime.year:
                return (True, asset_data)
            elif fecha_actual.month > last_datetime.month:
                return (True, asset_data)
            else:
                return (False, "")

        if interval == "1week":
            if fecha_actual.year > last_datetime.year:
                return (True, asset_data)
            elif fecha_actual.month > last_datetime.month:
                return (True, asset_data)
            elif numero_semana_actual > numero_semana_dato:
                return (True, asset_data)
            else:
                return (False, "")

        if interval == "1day":
            dia_semana = fecha_actual.weekday()
            if fecha_actual.year > last_datetime.year:
                return (True, asset_data)
            elif fecha_actual.month > last_datetime.month:
                return (True, asset_data)
            elif fecha_actual.day >= last_datetime.day + 3:
                return (True, asset_data)
            elif fecha_actual.day == last_datetime.day + 2 and (0 < dia_semana < 6):
                return (True, asset_data)
            elif (
                fecha_actual.day == last_datetime.day + 1
                and fecha_actual.hour >= 22
                and dia_semana < 5
            ):
                return (True, asset_data)

            else:
                return (False, "")

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

        if interval == "1day":
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

            dia_semana = fecha_despues.weekday()

            logger.info("Comprobamos si la fecha está dentro del horario de trading")
            if start_datetime < fecha_despues < end_datetime and dia_semana < 5:
                logger.info("Data needs to be updated")
                return (True, asset_data)

    def __obtain_updatable_assets(interval, assets):
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

    def __deleteAssetFromConfig(asset):
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

            config = AV_CoreData.__getConfig()
            # Delete asset from config asset array
            config["assets"].remove(asset)
            # Update config
            id = config["_id"]
            del config["_id"]
            conn.updateById(id, dict(config))
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __assestToBeDownloaded(interval, assets):
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
            fields = {"interval": interval, "symbol": {"$ne": "SPX"}}

            proyeccion = {
                "symbol": 1,
                "_id": 0,
            }  # 1 indica que deseas incluir el campo, 0 indica que no deseas incluirlo

            res = conn.findByMultipleFields(
                fields, get_all=True, custom=True, proyeccion=proyeccion
            )
            if not res:
                return (True, assets)

            symbols_array = [item["symbol"] for item in res]

            # Convertir los arrays en conjuntos
            conjunto_assets = set(assets)
            conjunto_symbols = set(symbols_array)

            # Obtener la diferencia de conjuntos
            resultado_set = conjunto_assets - conjunto_symbols

            # Convertir el resultado de nuevo en una lista si es necesario
            resultado_lista = list(resultado_set)
            conn.close()
            logger.info(
                f"Eliminados {len(assets)-len(resultado_lista)} activos que ya están en la base de datos"
            )
            logger.info(f"Activos a descargar: {len(resultado_lista)}")
            return (True, resultado_lista)
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __assestToBeUpdated(interval):
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
            fields = {"interval": interval, "symbol": {"$ne": "SPX"}}
            projection = {
                "interval": 1,
                "symbol": 1,
                "data": {
                    "$slice": 1
                },  # Obtener solo el primer elemento del array "data"
            }
            res = conn.findByMultipleFields(
                fields, get_all=True, custom=True, proyeccion=projection
            )
            if not res:
                return (True, [])

            conn.close()
            logger.info(f"Activos a actualizar: {len(res)}")
            return (True, res)
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __deleteAsset(asset):
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
            fields = {"symbol": asset["symbol"], "interval": asset["interval"]}
            # Delete asset from config asset array
            conn.deleteByMultipleField(custom=True, fields=fields)
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    def __getAsset(asset):
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
            fields = {"symbol": asset["symbol"], "interval": asset["interval"]}
            # Delete asset from config asset array
            element = conn.findByMultipleFields(custom=True, fields=fields)
            conn.close()
            return element
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)
