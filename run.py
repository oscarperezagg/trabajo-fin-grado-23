import sys
from src import iniciar_app, setup_app
from src.system.logging_config import logger

options = ["stats", "downloadSPX", "downloadStocks", "updateStocks", "updateSPX"]
main_text = """Si el programa se ejecuta sin argumentos, se ejecutará en modo normal.\n
Las opciones disponibles son:\n
    - stats: Muestra las métricas sobre la base de datos
    - downloadSPX: Descarga el histórico del SPX
    - downloadStocks: Descarga el histórico de las acciones del SPX
    - updateStocks: Actualiza el histórico de las acciones del SPX
    - updateSPX: Actualiza el histórico del SPX
    - setup: Configura la aplicación
    
Selecciona una opción (pulsa enter para ejecutar en modo normal): """
some_art = """ _____ _____ ____   ____  _____      ____  _  _   
|_   _|  ___/ ___| |___ \|___ /     |___ \| || |  
  | | | |_ | |  _    __) | |_ \ _____ __) | || |_ 
  | | |  _|| |_| |  / __/ ___) |_____/ __/|__   _|
  |_| |_|   \____| |_____|____/     |_____|  |_|  
  
  
  Made by: Óscar Pérez Arruti\n\n"""

if __name__ == "__main__":
    # Get system arguments
    try:
        print("\033c")
        print(some_art)

        selected_option = input(main_text)
        print("")
        if selected_option == "":
            iniciar_app()
        elif selected_option == "setup":
            setup_app()
        elif selected_option == "stats":
            iniciar_app(stats=True)
        elif selected_option == "downloadSPX":
            iniciar_app(DownloadSPX=True)
        elif selected_option == "downloadStocks":
            iniciar_app(DownloadStocks=True)
        elif selected_option == "updateStocks":
            iniciar_app(UpdateStocks=True)
        elif selected_option == "updateSPX":
            iniciar_app(UpdateSPX=True)
        else:
            print(f"Valid options are: {options}")
    except Exception as e:
        logger.critical("Fatal error occurred: %s", str(e))
    
