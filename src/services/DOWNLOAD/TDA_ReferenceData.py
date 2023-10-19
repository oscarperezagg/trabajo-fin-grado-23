# -*- coding: utf-8 -*-
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger

# Configure the logger



class TDA_ReferenceData:
    """
    Esta función implementa la lógica detrás de la descarga los "ReferenceData"
    del proveedor Twelve Data.

    """

    @staticmethod
    def getStocksList():
        """
        Esta función descarga los "Core Data" de todos los stocks disponibles
        en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING STOCKS LIST")
            logger.info("Performing request to /stocks")
            data = ReferenceData.stocks_list()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["stocks"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )

                conn.deleteWithFS("stockList.json")
                conn.insertWithFS(DB_DATA, "stockList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    @staticmethod
    def getIndicesList():
        """
        Esta función descarga los "Core Data" de todos los stocks disponibles
        en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING INDICES LIST")
            logger.info("Performing request to /indices")
            data = ReferenceData.get_indices()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["indices"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )
                conn.deleteWithFS("indicesList.json")
                conn.insertWithFS(DB_DATA, "indicesList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    @staticmethod
    def getForexList():
        """
        Esta función descarga los "Core Data" de todos los Forex disponibles
        en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING FOREX LIST")
            logger.info("Performing request to /forex_pairs")
            data = ReferenceData.forex_pairs_list()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["forex"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )
                conn.deleteWithFS("forexList.json")
                conn.insertWithFS(DB_DATA, "forexList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    @staticmethod
    def getCryptocurrenciesList():
        """
        Esta función descarga los "Core Data" de todos los "Cryptocurrencies" disponibles en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING CRYPTO LIST")
            logger.info("Performing request to /cryptocurrencies")
            data = ReferenceData.cryptocurrencies_list()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["cryptocurrencies"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )
                conn.deleteWithFS("cryptocurrenciesList.json")
                conn.insertWithFS(DB_DATA, "cryptocurrenciesList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    @staticmethod
    def getExchangesList():
        """
        Esta función descarga los "Core Data" de todos los "Exchanges" disponibles en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING EXHANGES LIST")
            logger.info("Performing request to /exchanges")
            data = ReferenceData.exchanges()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["exchanges"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )
                conn.deleteWithFS("exchangesList.json")
                conn.insertWithFS(DB_DATA, "exchangesList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)

    
    
    
    @staticmethod
    def getETFsList():
        """
        Esta función descarga los "Core Data" de todos los "ETFs" disponibles en el proveedor Twelve Data.

        :return: Un diccionario con los datos descargados.
        """
        try:
            logger.info("DOWNLOADING ETFs LIST")
            logger.info("Performing request to /etfs")
            data = ReferenceData.etf_list()
            if data[1].ok:
                logger.info("Data downloaded successfully")
                res_json = data[1].json()
                logger.info("Preparing data to be inserted into the database")
                DB_DATA = {}
                DB_DATA["etfs"] = res_json["data"]

                conn = MongoDbFunctions(
                    DATABASE["host"],
                    DATABASE["port"],
                    DATABASE["username"],
                    DATABASE["password"],
                    DATABASE["dbname"],
                )
                conn.deleteWithFS("etfsList.json")
                conn.insertWithFS(DB_DATA, "etfsList")
                conn.close()
                return (True, "")
            else:
                logger.error("An error occurred: %s", data[1].text)
                return (False, data[1].text)
        except Exception as e:
            logger.error("An error occurred: %s", str(e))
            return (False, e)
