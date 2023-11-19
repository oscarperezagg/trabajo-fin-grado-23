from datetime import datetime, timedelta
import time
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
from requests import Response
from .CleanDatabase import *

from .util import *

# Configure the logger

trading_hours = {
    "start": "15:30",
    "end": "23:00",
}

LIMIT = 2020


class AV_FundamentalData:
    def all_methods():
        AV_FundamentalData.getCompanyOverview()
        AV_FundamentalData.getIncomeStatement()
        AV_FundamentalData.getBalanceSheet()
        AV_FundamentalData.getCashFlow()
        AV_FundamentalData.getEarnings()

    def getCompanyOverview():
        """
        Get the company overview from the API. This functions should be executed once a day.
        There is no need for a specific update function.
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting company overview")

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CompanyOverview",
        )

        proyeccion = {"symbol": 1, "_id": 0}

        already_contained = conn.findByMultipleFields(
            {}, custom=True, get_all=True, proyeccion=proyeccion
        )
        valores_symbol = [documento["symbol"] for documento in already_contained]

        not_downloaded_assets = [
            asset for asset in assets if asset not in valores_symbol
        ]
        all_assets = [not_downloaded_assets, valores_symbol]
        # Get the company overview for each asset
        n = 0
        for half_assets in all_assets:
            tipo = "pending" if n == 0 else "already downloaded"
            print("")
            logger.warning(f"Getting company overview for {tipo} assets")
            print("")
            for asset in half_assets:
                try:
                    logger.info(f"Getting company overview for {asset}")
                    # Get the company overview
                    params = {
                        "symbol": asset,
                        "apikey": ALPHA_VANTAGE_API_KEY,
                    }
                    check = AV_FundamentalData.__anotherCall()
                    if not check[0]:
                        return check

                    response = FundamentalData.get_company_overview(**params)
                    AV_FundamentalData.__oneMoreCall()
                    # Check if the response is valid
                    if response[0]:
                        # Insert the data into the database
                        conn = MongoDbFunctions(
                            DATABASE["host"],
                            DATABASE["port"],
                            DATABASE["username"],
                            DATABASE["password"],
                            DATABASE["dbname"],
                            "CompanyOverview",
                        )
                        data = response[1].json()
                        if data != {}:
                            del data["Symbol"]
                            data["symbol"] = asset
                            conn.deleteByField("symbol", asset)
                            conn.insert(data)
                        else:
                            logger.error(f"No company overview found for {asset}")
                    else:
                        logger.error("An error occurred: %s", response[1])
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
                    continue
            n += 1

        logger.info("Company overview downloaded")

    def getIncomeStatement():
        """
        Get the income statement from the API. This functions should be executed once a day.
        There is no need for a specific update function.
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting income statement")

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "IncomeStatement",
        )

        proyeccion = {"symbol": 1, "_id": 0}

        already_contained = conn.findByMultipleFields(
            {}, custom=True, get_all=True, proyeccion=proyeccion
        )
        valores_symbol = [documento["symbol"] for documento in already_contained]

        not_downloaded_assets = [
            asset for asset in assets if asset not in valores_symbol
        ]
        all_assets = [not_downloaded_assets, valores_symbol]
        # Get the income statement for each asset
        n = 0
        for half_assets in all_assets:
            tipo = "pending" if n == 0 else "already downloaded"
            print("")
            logger.warning(f"Getting Income Statement for {tipo} assets")
            print("")

            for asset in half_assets:
                try:
                    conn = MongoDbFunctions(
                        DATABASE["host"],
                        DATABASE["port"],
                        DATABASE["username"],
                        DATABASE["password"],
                        DATABASE["dbname"],
                        "IncomeStatement",
                    )

                    logger.info(f"Getting Income Statement for {asset}")
                    # Get the income statement
                    params = {
                        "symbol": asset,
                        "apikey": ALPHA_VANTAGE_API_KEY,
                    }
                    check = AV_FundamentalData.__anotherCall()
                    if not check[0]:
                        return check

                    if n == 1:
                        # Calcula la fecha de hoy
                        today = datetime.now()

                        symbolIncomeStatement = conn.findByField("symbol", asset)

                        last_yearly_date = symbolIncomeStatement["annualReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_quarterly_date = symbolIncomeStatement["quarterlyReports"][
                            0
                        ]["fiscalDateEnding"]
                        last_yearly_datetime = datetime.strptime(
                            last_yearly_date, "%Y-%m-%d"
                        )
                        last_quarterly_datetime = datetime.strptime(
                            last_quarterly_date, "%Y-%m-%d"
                        )

                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        eleven_months_later = last_yearly_datetime + timedelta(
                            days=11 * 30
                        )  # Aproximadamente 30 días por mes
                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        two_months_and_half = last_quarterly_datetime + timedelta(
                            days=(2 * 30 + 10)
                        )  # Aproximadamente 30 días por mes

                        # Compara las fechas
                        check_yearly = today < eleven_months_later
                        check_quarterly = today < two_months_and_half

                        if check_quarterly and check_yearly:
                            logger.warning("No es necesario actualizar los datos")
                            continue

                    response = FundamentalData.get_income_statement(**params)
                    AV_FundamentalData.__oneMoreCall()
                    # Check if the response is valid
                    if response[0]:
                        # Insert the data into the database
                        data = response[1].json()
                        if data != {}:
                            if n == 1:
                                # We add just the new data
                                new_yearly = 0
                                for item in data["annualReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )

                                    if temp_date <= last_yearly_datetime:
                                        break
                                    new_yearly += 1

                                new_quarterly = 0
                                for item in data["quarterlyReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )
                                    if temp_date <= last_quarterly_datetime:
                                        break
                                    new_quarterly += 1

                                if new_yearly > 0:
                                    temp_data = symbolIncomeStatement["annualReports"]
                                    symbolIncomeStatement["annualReports"] = data[
                                        "annualReports"
                                    ][:new_yearly]
                                    symbolIncomeStatement["annualReports"].extend(
                                        temp_data
                                    )

                                if new_quarterly > 0:
                                    temp_data = symbolIncomeStatement[
                                        "quarterlyReports"
                                    ]
                                    symbolIncomeStatement["quarterlyReports"] = data[
                                        "quarterlyReports"
                                    ][:new_quarterly]
                                    symbolIncomeStatement["quarterlyReports"].extend(
                                        temp_data
                                    )

                                conn.deleteByField("symbol", asset)

                            conn.insert(data)
                        else:
                            logger.error(f"No income statement found for {asset}")
                    else:
                        logger.error("An error occurred: %s", response[1])
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
            n += 1

        logger.info("Income statement downloaded")

    def getBalanceSheet():
        """
        Get the balance sheet from the API. This functions should be executed once a day.
        There is no need for a specific update function.
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting balance sheet")

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "BalanceSheet",
        )

        proyeccion = {"symbol": 1, "_id": 0}

        already_contained = conn.findByMultipleFields(
            {}, custom=True, get_all=True, proyeccion=proyeccion
        )
        valores_symbol = [documento["symbol"] for documento in already_contained]

        not_downloaded_assets = [
            asset for asset in assets if asset not in valores_symbol
        ]
        all_assets = [not_downloaded_assets, valores_symbol]
        # Get the balance sheet for each asset
        n = 0
        for half_assets in all_assets:
            tipo = "pending" if n == 0 else "already downloaded"
            print("")
            logger.warning(f"Getting Balance Sheet {tipo} assets")
            print("")

            for asset in half_assets:
                try:
                    conn = MongoDbFunctions(
                        DATABASE["host"],
                        DATABASE["port"],
                        DATABASE["username"],
                        DATABASE["password"],
                        DATABASE["dbname"],
                        "BalanceSheet",
                    )

                    logger.info(f"Getting Balance Sheet for {asset}")
                    # Get the balance sheet
                    params = {
                        "symbol": asset,
                        "apikey": ALPHA_VANTAGE_API_KEY,
                    }
                    check = AV_FundamentalData.__anotherCall()
                    if not check[0]:
                        return check

                    if n == 1:
                        # Calcula la fecha de hoy
                        today = datetime.now()

                        symbolBalanceSheet = conn.findByField("symbol", asset)

                        last_yearly_date = symbolBalanceSheet["annualReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_quarterly_date = symbolBalanceSheet["quarterlyReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_yearly_datetime = datetime.strptime(
                            last_yearly_date, "%Y-%m-%d"
                        )
                        last_quarterly_datetime = datetime.strptime(
                            last_quarterly_date, "%Y-%m-%d"
                        )

                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        eleven_months_later = last_yearly_datetime + timedelta(
                            days=11 * 30
                        )  # Aproximadamente 30 días por mes
                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        two_months_and_half = last_quarterly_datetime + timedelta(
                            days=(2 * 30 + 10)
                        )  # Aproximadamente 30 días por mes

                        # Compara las fechas
                        check_yearly = today < eleven_months_later
                        check_quarterly = today < two_months_and_half

                        if check_quarterly and check_yearly:
                            logger.warning("No es necesario actualizar los datos")
                            continue

                    response = FundamentalData.get_balance_sheet(**params)
                    AV_FundamentalData.__oneMoreCall()
                    # Check if the response is valid
                    if response[0]:
                        # Insert the data into the database
                        data = response[1].json()
                        if data != {}:
                            if n == 1:
                                # We add just the new data
                                new_yearly = 0
                                for item in data["annualReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )

                                    if temp_date <= last_yearly_datetime:
                                        break
                                    new_yearly += 1

                                new_quarterly = 0
                                for item in data["quarterlyReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )
                                    if temp_date <= last_quarterly_datetime:
                                        break
                                    new_quarterly += 1

                                if new_yearly > 0:
                                    temp_data = symbolBalanceSheet["annualReports"]
                                    symbolBalanceSheet["annualReports"] = data[
                                        "annualReports"
                                    ][:new_yearly]
                                    symbolBalanceSheet["annualReports"].extend(
                                        temp_data
                                    )

                                if new_quarterly > 0:
                                    temp_data = symbolBalanceSheet["quarterlyReports"]
                                    symbolBalanceSheet["quarterlyReports"] = data[
                                        "quarterlyReports"
                                    ][:new_quarterly]
                                    symbolBalanceSheet["quarterlyReports"].extend(
                                        temp_data
                                    )

                                conn.deleteByField("symbol", asset)

                            conn.insert(data)
                        else:
                            logger.error(f"No balance sheet found for {asset}")
                    else:
                        logger.error("An error occurred: %s", response[1])
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
            n += 1

        logger.info("Balance sheet downloaded")

    def getCashFlow():
        """
        Get the cash flow from the API. This functions should be executed once a day.
        There is no need for a specific update function.
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting cash flow")

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CashFlow",
        )

        proyeccion = {"symbol": 1, "_id": 0}

        already_contained = conn.findByMultipleFields(
            {}, custom=True, get_all=True, proyeccion=proyeccion
        )
        valores_symbol = [documento["symbol"] for documento in already_contained]

        not_downloaded_assets = [
            asset for asset in assets if asset not in valores_symbol
        ]
        all_assets = [not_downloaded_assets, valores_symbol]
        # Get the cash flow for each asset
        n = 0
        for half_assets in all_assets:
            tipo = "pending" if n == 0 else "already downloaded"
            print("")
            logger.warning(f"Getting Cash Flow for {tipo} assets")
            print("")

            for asset in half_assets:
                try:
                    conn = MongoDbFunctions(
                        DATABASE["host"],
                        DATABASE["port"],
                        DATABASE["username"],
                        DATABASE["password"],
                        DATABASE["dbname"],
                        "CashFlow",
                    )

                    logger.info(f"Getting Cash Flow for {asset}")
                    # Get the cash flow
                    params = {
                        "symbol": asset,
                        "apikey": ALPHA_VANTAGE_API_KEY,
                    }
                    check = AV_FundamentalData.__anotherCall()
                    if not check[0]:
                        return check

                    if n == 1:
                        # Calcula la fecha de hoy
                        today = datetime.now()

                        symbolCashFlow = conn.findByField("symbol", asset)

                        last_yearly_date = symbolCashFlow["annualReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_quarterly_date = symbolCashFlow["quarterlyReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_yearly_datetime = datetime.strptime(
                            last_yearly_date, "%Y-%m-%d"
                        )
                        last_quarterly_datetime = datetime.strptime(
                            last_quarterly_date, "%Y-%m-%d"
                        )

                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        eleven_months_later = last_yearly_datetime + timedelta(
                            days=11 * 30
                        )  # Aproximadamente 30 días por mes
                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        two_months_and_half = last_quarterly_datetime + timedelta(
                            days=(2 * 30 + 10)
                        )  # Aproximadamente 30 días por mes

                        # Compara las fechas
                        check_yearly = today < eleven_months_later
                        check_quarterly = today < two_months_and_half

                        if check_quarterly and check_yearly:
                            logger.warning("No es necesario actualizar los datos")
                            continue

                    response = FundamentalData.get_cash_flow(**params)
                    AV_FundamentalData.__oneMoreCall()
                    # Check if the response is valid
                    if response[0]:
                        # Insert the data into the database
                        data = response[1].json()
                        if data != {}:
                            if n == 1:
                                # We add just the new data
                                new_yearly = 0
                                for item in data["annualReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )

                                    if temp_date <= last_yearly_datetime:
                                        break
                                    new_yearly += 1

                                new_quarterly = 0
                                for item in data["quarterlyReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )
                                    if temp_date <= last_quarterly_datetime:
                                        break
                                    new_quarterly += 1

                                if new_yearly > 0:
                                    temp_data = symbolCashFlow["annualReports"]
                                    symbolCashFlow["annualReports"] = data[
                                        "annualReports"
                                    ][:new_yearly]
                                    symbolCashFlow["annualReports"].extend(temp_data)

                                if new_quarterly > 0:
                                    temp_data = symbolCashFlow["quarterlyReports"]
                                    symbolCashFlow["quarterlyReports"] = data[
                                        "quarterlyReports"
                                    ][:new_quarterly]
                                    symbolCashFlow["quarterlyReports"].extend(temp_data)

                                conn.deleteByField("symbol", asset)

                            conn.insert(data)
                        else:
                            logger.error(f"No Cash Flow found for {asset}")
                    else:
                        logger.error("An error occurred: %s", response[1])
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
            n += 1

        logger.info("Cash flow downloaded")

    def getEarnings():
        """
        Get the Earnings from the API. This functions should be executed once a day.
        There is no need for a specific update function.
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting Earnings")

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "Earnings",
        )

        proyeccion = {"symbol": 1, "_id": 0}

        already_contained = conn.findByMultipleFields(
            {}, custom=True, get_all=True, proyeccion=proyeccion
        )
        valores_symbol = [documento["symbol"] for documento in already_contained]

        not_downloaded_assets = [
            asset for asset in assets if asset not in valores_symbol
        ]
        all_assets = [not_downloaded_assets, valores_symbol]
        # Get the Earnings for each asset
        n = 0
        for half_assets in all_assets:
            tipo = "pending" if n == 0 else "already downloaded"
            print("")
            logger.warning(f"Getting Earnings for {tipo} assets")
            print("")

            for asset in half_assets:
                try:
                    conn = MongoDbFunctions(
                        DATABASE["host"],
                        DATABASE["port"],
                        DATABASE["username"],
                        DATABASE["password"],
                        DATABASE["dbname"],
                        "Earnings",
                    )

                    logger.info(f"Getting Earnings for {asset}")
                    # Get the Earnings
                    params = {
                        "symbol": asset,
                        "apikey": ALPHA_VANTAGE_API_KEY,
                    }
                    check = AV_FundamentalData.__anotherCall()
                    if not check[0]:
                        return check

                    if n == 1:
                        # Calcula la fecha de hoy
                        today = datetime.now()

                        symbolCashFlow = conn.findByField("symbol", asset)

                        last_yearly_date = symbolCashFlow["annualReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_quarterly_date = symbolCashFlow["quarterlyReports"][0][
                            "fiscalDateEnding"
                        ]
                        last_yearly_datetime = datetime.strptime(
                            last_yearly_date, "%Y-%m-%d"
                        )
                        last_quarterly_datetime = datetime.strptime(
                            last_quarterly_date, "%Y-%m-%d"
                        )

                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        eleven_months_later = last_yearly_datetime + timedelta(
                            days=11 * 30
                        )  # Aproximadamente 30 días por mes
                        # Calcula la fecha que está 11 meses después de last_yearly_date
                        two_months_and_half = last_quarterly_datetime + timedelta(
                            days=(2 * 30 + 10)
                        )  # Aproximadamente 30 días por mes

                        # Compara las fechas
                        check_yearly = today < eleven_months_later
                        check_quarterly = today < two_months_and_half

                        if check_quarterly and check_yearly:
                            logger.warning("No es necesario actualizar los datos")
                            continue

                    response = FundamentalData.get_earnings(**params)
                    AV_FundamentalData.__oneMoreCall()
                    # Check if the response is valid
                    if response[0]:
                        # Insert the data into the database
                        data = response[1].json()
                        if data != {}:
                            if n == 1:
                                # We add just the new data
                                new_yearly = 0
                                for item in data["annualReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )

                                    if temp_date <= last_yearly_datetime:
                                        break
                                    new_yearly += 1

                                new_quarterly = 0
                                for item in data["quarterlyReports"]:
                                    temp_date = datetime.strptime(
                                        item["fiscalDateEnding"], "%Y-%m-%d"
                                    )
                                    if temp_date <= last_quarterly_datetime:
                                        break
                                    new_quarterly += 1

                                if new_yearly > 0:
                                    temp_data = symbolCashFlow["annualReports"]
                                    symbolCashFlow["annualReports"] = data[
                                        "annualReports"
                                    ][:new_yearly]
                                    symbolCashFlow["annualReports"].extend(temp_data)

                                if new_quarterly > 0:
                                    temp_data = symbolCashFlow["quarterlyReports"]
                                    symbolCashFlow["quarterlyReports"] = data[
                                        "quarterlyReports"
                                    ][:new_quarterly]
                                    symbolCashFlow["quarterlyReports"].extend(temp_data)

                                conn.deleteByField("symbol", asset)

                            conn.insert(data)
                        else:
                            logger.error(f"No Earnings found for {asset}")
                    else:
                        logger.error("An error occurred: %s", response[1])
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
            n += 1

        logger.info("Earnings downloaded")

    ###############
    #   SUPPORT   #
    ###############

    def __anotherCall():
        conn = None
        try:
            config_twelve_data_api = getConfig(api_name="alphavantage")

            # Comprobamos si el tiempo de modificación es de hace un año
            last_modification_date = config_twelve_data_api.get("fecha_modificacion")
            if last_modification_date:
                current_date = datetime.now()

                # Comprueba si la diferencia de tiempo es mayor que la duración mínima
                if current_date.day > last_modification_date.day:
                    logger.debug("La fecha de modificación es de hace un día.")
                    AV_FundamentalData.__DailyCallTOZero()
                    AV_FundamentalData.__minuteCallTOZero()
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
                    logger.debug(
                        "La fecha de modificación es de hace más de un minuto y medio."
                    )
                    AV_FundamentalData.__minuteCallTOZero()
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
                AV_FundamentalData.__minuteCallTOZero()
                logger.warning("Esperando 60 segundos...")
                time.sleep(80)
            return (True, "")
        except Exception as e:
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
            logger.debug("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
            logger.debug("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_por_minuto"] = 0
            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)
            logger.debug("Configuración actualizada")
            conn.close()
            logger.debug("Llamadas por minuto zeorizadas")
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
            logger.debug("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
            logger.debug("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_diarias"] = 0
            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)
            logger.debug("Llamadas diarias zeorizadas")
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

            logger.debug("Obteniendo configuración de la API Alpha Vantaje")
            config_twelve_data_api = conn.findByField("nombre_api", "alphavantage")
            logger.debug("Configuración obtenida")
            config_twelve_data_api["llamadas_actuales_diarias"] += 1
            config_twelve_data_api["llamadas_actuales_por_minuto"] += 1
            config_twelve_data_api["fecha_modificacion"] = datetime.now()

            conn.updateById(config_twelve_data_api["_id"], config_twelve_data_api)

            logger.debug("Registry updated successfully")
            conn.close()
            return (True, "")
        except Exception as e:
            if conn:
                conn.close()
            logger.error("An error occurred: %s", str(e))
            return (False, e)
