import os
from src.lib.MONGODB import *
from src.system.secret import *
from src.system.logging_config import logger
from src.modules.ALGORITHM_MODULE import parameters
import threading
from tqdm import tqdm


INTERVAL = "1day"


class computation:
    #############################################################
    #######################  MOMENTUM  ##########################
    #############################################################

    @staticmethod
    def computeData(testing=False):
        logger.info("Comenzando el cálculo de los datos")
        logger.setLevel("INFO")

        spx = computation.standard_and_poors()

        validStocks = computation.getValidStocks()
        logger.info("Obteninedo dirección de la carpeta temporal")
        path = computation.getTempPath()
        print("\n")
        storingThreats = []
        for stock in tqdm(validStocks,  desc=f"Procesando stocks"):

            standard = spx.copy()
            t = computation.computeSingleData(stock, path, standard, testing)
            storingThreats.append(t)
            
        print("\n")
        logger.info("Esperando a que los threads terminen")
        for t in storingThreats:
            t.join()

        return validStocks

    def computeSingleData(stock, path, spx, testing):
        logger.debug("---> Procesando %s", stock)
        data = computation.getStock(stock)
        
        # Checking de datos superiores
        data_last_date = data.iloc[-1]["date"]
        spx_last_date = spx.iloc[-1]["date"]
        if spx_last_date > data_last_date:
            spx = spx[(spx["date"] <= data_last_date)]
        elif data_last_date > spx_last_date:
            data = data[(data["date"] <= spx_last_date)]

        logger.debug("---> Calculando el SMA de 50 y 200 períodos")
        parameters.simplemovingaverage(data, 200)
        parameters.simplemovingaverage(data, 50)

        logger.debug("---> Calculando el RSI de 14 períodos")
        parameters.relativestregthindex(data, 14)

        logger.debug("---> Calculando las betas históricas de 5 año")
        parameters.beta(data, spx, testing)

        logger.debug(
            "---> Asignando la fecha de presentación de resultados a los datos de 15 minutos"
        )
        parameters.asignReportPresentationDate(stock, data)

        logger.debug(
            "---> Calculando el movimiento de la acción tras presentación de resultados a los datos de 15 minutos"
        )
        parameters.asignMovement(stock, data)

        # Creando un thread
        t = threading.Thread(target=computation.storeData, args=(data, stock, path))

        t.start()

        return t

        # El argumento index=False evita que se guarde el índice del DataFrame

    #############################################################
    #######################  SUPPORT  ##########################
    #############################################################

    def getValidStocks():
        """
        Obtenemos los stocks que tiene valores en el intervalo 15 minutos,
        en el intervalo 1 día y que tienen datos en incomeStatement.
        """
        logger.info("---> Obteniendo los stocks válidos")
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CoreData",
        )

        dailyAsset = conn.findByMultipleFields(
            fields={"interval": "1day", "symbol": {"$ne": "SPX"}},
            custom=True,
            get_all=True,
            proyeccion={"_id": 0, "symbol": 1},
        )

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "IncomeStatement",
        )

        assetsWithIncomeStatement = conn.findByMultipleFields(
            fields={"symbol": {"$ne": "SPX"}},
            custom=True,
            get_all=True,
            proyeccion={"_id": 0, "symbol": 1},
        )

        dailyAsset = [asset["symbol"] for asset in dailyAsset]
        assetsWithIncomeStatement = [
            asset["symbol"] for asset in assetsWithIncomeStatement
        ]

        # Convierte las listas en conjuntos

        dailyAsset_set = set(dailyAsset)
        assetsWithIncomeStatement_set = set(assetsWithIncomeStatement)

        # Encuentra la intersección
        interseccion = dailyAsset_set & assetsWithIncomeStatement_set
        logger.info("---> Se han encontrado %s stocks válidos", len(interseccion))
        # Convierte el resultado de nuevo en una lista si es necesario
        return list(interseccion)

    def getStock(symbol):
        logger.debug("---> Obteniendo los datos de %s", symbol)
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CoreData",
        )

        data = conn.findByMultipleFields(
            fields={"symbol": symbol, "interval": INTERVAL}, custom=True
        )
        logger.debug("---> Se han obtenido los datos de %s", symbol)
        data = parameters.formatData(data)
        logger.debug("---> Se han formateado los datos")
        return data

    def getTempPath():
        # Obtén la ruta del directorio actual donde se encuentra el archivo.py
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construye la ruta al directorio "temp" que está fuera del directorio actual
        ruta_temp = os.path.abspath(os.path.join(directorio_actual, "..", "..", "temp"))

        # Convierte la ruta a una ruta absoluta (opcional)
        ruta_temp_absoluta = os.path.abspath(ruta_temp)

        return ruta_temp_absoluta

    def storeData(data, stock, path):
        data.to_pickle(f"{path}/{stock}.pkl")
        
    @staticmethod
    def standard_and_poors():
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CoreData",
        )
        spx = conn.findByMultipleFields(
            fields={"symbol": "SPX", "interval": "1day"}, custom=True
        )
        spx = parameters.formatData(spx)
        return spx
