import logging
import threading

from src.modules import *
from src.modules.DATA_MODULE import *


def main(stats=False,DownloadSPX=False,DownloadStocks=False,UpdateStocks=False,UpdateSPX=False):
    # Purge data
    cleanDatabase.purgeData()
    # Show stats
    logger.setLevel(logging.INFO)
    # Temporal
    if stats:
        Scheduling.schedule_stats('seconds', 10)
    elif DownloadStocks:
        AV_CoreData.downloadAsset()
    elif UpdateStocks:
        AV_CoreData.updateAssets()
    elif DownloadSPX:
        TDA_CoreData.downloadAsset()
    elif UpdateSPX:
        TDA_CoreData.updateAssets()        
    else:
        logger.info('Starting application')
        # Schedule the task of downloading the SPX
        Scheduling.schedule_spx_download('10:00')
        # Schedule the task of updating the SPX
        Scheduling.schedule_spx_update('hour', 1)
        # Schedule the task of downloading stocks
        Scheduling.schedule_stock_download('11:00')
        # Schedule the task of updating stocks
        Scheduling.schedule_stocks_update('hour', 1)

    
def ejecutar_check_for_task():
    while True:
        Scheduling.checkForTask()
        time.sleep(1)  # Añade un pequeño retraso para evitar un uso excesivo de CPU


    

    
def iniciar_app(stats=False,DownloadSPX=False,DownloadStocks=False,UpdateStocks=False,UpdateSPX=False):
    print("")
    logger.debug("Crear un hilo para la lógica de la aplicación")
    app_thread = threading.Thread(target=main, args=(stats,DownloadSPX,DownloadStocks,UpdateStocks,UpdateSPX))
    app_thread.start()  # Iniciar el hilo de la aplicación
    print("")
    logger.debug("Crear un hilo para ejecutar Scheduling.checkForTask() en segundo plano")
    check_for_task_thread = threading.Thread(target=ejecutar_check_for_task)
    check_for_task_thread.start()  # Iniciar el hilo de Scheduling.checkForTask()
    print("")






