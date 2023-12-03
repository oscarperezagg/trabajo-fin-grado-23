import ipaddress
import logging
import threading
import os

from src.modules import *
from src.modules.DATA_MODULE import *
from src.lib import *
from src.system.secret import *
from src.system.migrate import *


some_art = """ _____ _____ ____   ____  _____      ____  _  _   
|_   _|  ___/ ___| |___ \|___ /     |___ \| || |  
  | | | |_ | |  _    __) | |_ \ _____ __) | || |_ 
  | | |  _|| |_| |  / __/ ___) |_____/ __/|__   _|
  |_| |_|   \____| |_____|____/     |_____|  |_|  
  
  
  Made by: Óscar Pérez Arruti\n\n"""


def main(mode=None):
    # Purge data
    cleanDatabase.purgeData()
    # Show stats
    logger.setLevel(logging.INFO)

    print("\033c")
    print(some_art)
    # Temporal
    if mode == "computeAndResults":
        validStocks = computation.computeData()
        signals.signals(validStocks)
    elif mode == "compute":
        computation.computeData()
    elif mode == "lastResults":
        validStocks = computation.getValidStocks()
        signals.signals(validStocks)
    elif mode == "stats":
        Scheduling.schedule_stats("seconds", 10)
    elif mode == "DownloadStocks":
        AV_CoreData.downloadAsset()
    elif mode == "UpdateStocks":
        AV_CoreData.updateAssets()
    elif mode == "DownloadSPX":
        TDA_CoreData.downloadAsset()
    elif mode == "UpdateSPX":
        TDA_CoreData.updateAssets()
    elif mode == "companyOverview":
        AV_FundamentalData.getCompanyOverview()
    elif mode == "IncomeStatement":
        AV_FundamentalData.getIncomeStatement()
    elif mode == "BalanceSheet":
        AV_FundamentalData.getBalanceSheet()
    elif mode == "CashFlow":
        AV_FundamentalData.getCashFlow()
    elif mode == "Earnings":
        AV_FundamentalData.getEarnings()
    else:
        logger.info("Starting application")
        # Schedule the task of downloading the SPX
        Scheduling.schedule_spx_download("7:00")
        # Schedule the task of updating the SPX
        Scheduling.schedule_spx_update("hour", 1)
        # Schedule the task of downloading stocks
        Scheduling.schedule_stock_download("8:00")
        # Schedule the task of updating stocks
        Scheduling.schedule_stocks_update("hour", 1)
        # Schedule the task of getting company overview
        Scheduling.schedule_download_fundamental_data("6:00")


def ejecutar_check_for_task():
    while True:
        Scheduling.checkForTask()
        time.sleep(1)  # Añade un pequeño retraso para evitar un uso excesivo de CPU


def iniciar_app(mode=None):
    print("")
    logger.debug("Crear un hilo para la lógica de la aplicación")
    app_thread = threading.Thread(
        target=main,
        args=(mode,),
    )

    app_thread.start()  # Iniciar el hilo de la aplicación
    print("")
    if mode == "": 
        logger.debug(
            "Crear un hilo para ejecutar Scheduling.checkForTask() en segundo plano"
        )
        check_for_task_thread = threading.Thread(target=ejecutar_check_for_task)
        check_for_task_thread.start()  # Iniciar el hilo de Scheduling.checkForTask()
        print("")


def setup_app(host="127.0.0.1", port="27017", user=None, password=None):
    # Ask for host and port

    # Comprorba si el archivo dentro secret.py dentro de la carpeta system existe
    # Obtiene la ruta al directorio actual (donde se encuentra el archivo en ejecución)
    directorio_actual = os.path.dirname(os.path.abspath(__file__))

    # Construye la ruta completa al archivo secret.py dentro de la carpeta "system"
    ruta_archivo_secret = os.path.join(directorio_actual, "system", "secret.py")

    # Verifica si el archivo existe
    if not os.path.exists(ruta_archivo_secret):
        msg = """
        
Asegurese de que el archivo secret.py exista. Puede encontrar un ejemplo 
(secret_template.py) en la misma carpeta donde secret.py debería guardarse.

Configure bien los siguiente campos:

- ALPHA_VANTAGE_API_KEY
- TWELVE_DATA_API_KEY
- DATABASE
        """
        logger.error(msg)
        return

    check = input("Want to change default host = 127.0.0.1 and port = 27001 (y/n): ")
    if check == "y":
        host = input("Host: ")
        port = input("Port: ")

    # Ask for user + pass
    check = input("Is there a user and password? (y/n): ")
    if check == "y":
        user = input("User: ")
        password = input("Password: ")
    logger.info("Configurando la aplicación")
    conn = MongoDbFunctions(host, port, user, password)
    # Comprobamos si la base de datos existe y si no creamos
    conn.setDatabase(DATABASE["dbname"])
    # Creamos las colecciones
    conn.createCollections(COLLECTIONS)
    # Creamos los documentos de prueba
    conn.defaultDocs(TEST_DOCS)
    logger.info("Configuración completada")
