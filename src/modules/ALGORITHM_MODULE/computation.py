import os
from src.lib.MONGODB import *
from src.system.secret import *
from src.system.logging_config import logger
from src.modules.ALGORITHM_MODULE import parameters
import threading
from tqdm import tqdm


INTERVAL = "15min"


class computation:
    #############################################################
    #######################  MOMENTUM  ##########################
    #############################################################

    @staticmethod
    def computeData(testing=False):
        logger.info("Comenzando el cálculo de los datos")
        logger.setLevel("INFO")
        validStocks = computation.getValidStocks()
        logger.info("Obteninedo dirección de la carpeta temporal")
        path = computation.getTempPath()
        print("\n")
        storingThreats = []
        for stock in tqdm(validStocks, desc="Procesando stocks"):

            print(f"Procesando {stock}")
            t = computation.computeSingleData(stock, path, testing)
            storingThreats.append(t)
        print("\n")
        logger.info("Esperando a que los threads terminen")
        for t in storingThreats:
            t.join()

        return validStocks

    def computeSingleData(stock, path, testing=False):
        logger.debug("---> Procesando %s", stock)
        data = computation.getStock(stock, testing)

        logger.debug("---> Calculando el SMA de 50 y 200 períodos")
        parameters.simplemovingaverage(data, 200)
        parameters.simplemovingaverage(data, 50)

        logger.debug("---> Calculando el RSI de 14 períodos")
        parameters.relativestregthindex(data, 14)

        logger.debug("---> Calculando las betas históricas de 5 año")
        year = data.iloc[0]["date"].year
        betas = parameters.beta(stock, limit=str(year))

        logger.debug("---> Aplicando las betas históricas a los datos de 15 minutos")
        parameters.apply_beta(data, betas)

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

        minute15Assets = conn.findByMultipleFields(
            fields={"interval": INTERVAL, "symbol": {"$ne": "SPX"}},
            custom=True,
            get_all=True,
            proyeccion={"_id": 0, "symbol": 1},
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

        minute15Assets = [asset["symbol"] for asset in minute15Assets]
        dailyAsset = [asset["symbol"] for asset in dailyAsset]
        assetsWithIncomeStatement = [
            asset["symbol"] for asset in assetsWithIncomeStatement
        ]

        # Convierte las listas en conjuntos
        minute15Assets_set = set(minute15Assets)
        dailyAsset_set = set(dailyAsset)
        assetsWithIncomeStatement_set = set(assetsWithIncomeStatement)

        # Encuentra la intersección
        interseccion = (
            minute15Assets_set & dailyAsset_set & assetsWithIncomeStatement_set
        )
        logger.info("---> Se han encontrado %s stocks válidos", len(interseccion))
        # Convierte el resultado de nuevo en una lista si es necesario
        return list(interseccion)

    def getStock(symbol, testing):
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
        if not testing:
            data = data.iloc[-380:]
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
