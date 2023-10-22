import threading

from src.services import *
from src.services.DOWNLOAD import *
from src.interface import *

def main(justStats=False):
    # Purge data
    cleanDatabase.purgeData()
    # Show stats
    Stats.get_stats()
    # Temporal
    if not justStats:
        logger.info('Starting application')
        # Schedule the task of downloading the SPX
        Scheduling.schedule_spx_download('10:00')
        # Schedule the task of updating the SPX
        Scheduling.schedule_spx_update('hour', 1)
        # Schedule the task of downloading stocks
        Scheduling.schedule_stock_download('12:28')
        # Schedule the task of updating stocks
        # Scheduling.schedule_stocks_update('hour', 1)
    else:
        Scheduling.schedule_stats('seconds', 10)

    while True:
        Scheduling.checkForTask()
    
def ejecutar_check_for_task():
    while True:
        Scheduling.checkForTask()
        time.sleep(1)  # Añade un pequeño retraso para evitar un uso excesivo de CPU


    

    
def iniciar_app(justStats=False):
    print("")
    logger.debug("Crear un hilo para la lógica de la aplicación")
    app_thread = threading.Thread(target=main, args=(justStats,))
    app_thread.start()  # Iniciar el hilo de la aplicación
    print("")
    logger.debug("Crear un hilo para ejecutar Scheduling.checkForTask() en segundo plano")
    check_for_task_thread = threading.Thread(target=ejecutar_check_for_task)
    check_for_task_thread.start()  # Iniciar el hilo de Scheduling.checkForTask()
    print("")

    #logger.debug("Iniciar la aplicación tkinter en el hilo principal")
    #iniciar_tkinter()





