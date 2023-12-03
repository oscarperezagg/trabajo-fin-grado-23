import sys
from src import iniciar_app, setup_app, migrate
from src.system.logging_config import logger


main_text = """Si el programa se ejecuta sin argumentos, se ejecutará en modo normal.\n
Opciones generales:
    
    - full: Calcular sobre los últimos datos y obtener señales
    - compute: Calcular sobre los últimos datos
    - signals: Obtener señales sobre los últimos datos calculados

Opciones de descarga de históricos:

    - downloadSPX: Descarga el histórico del SPX
    - downloadStocks: Descarga el histórico de las acciones del SPX
    - updateStocks: Actualiza el histórico de las acciones del SPX
    - updateSPX: Actualiza el histórico del SPX

Options de descarga de datos fundamentales:

    - companyOverview: Descargsa la información de las acciones del SPX
    - IncomeStatement: Descarga el estado de resultados de las acciones del SPX
    - BalanceSheet: Descarga el balance de las acciones del SPX
    - CashFlow: Descarga el flujo de caja de las acciones del SPX
    - Earnings: Descarga las ganancias de las acciones del SPX
    
Opciones del sistema: 

    - stats: Muestra las métricas sobre la base de datos
    - setup: Configura la aplicación
    - migrate: Migrar la base de datos
    
Selecciona una opción (pulsa enter para ejecutar en modo normal): """
some_art = """ _____ _____ ____   ____  _____      ____  _  _   
|_   _|  ___/ ___| |___ \|___ /     |___ \| || |  
  | | | |_ | |  _    __) | |_ \ _____ __) | || |_ 
  | | |  _|| |_| |  / __/ ___) |_____/ __/|__   _|
  |_| |_|   \____| |_____|____/     |_____|  |_|  
  
  
  Made by: Óscar Pérez Arruti\n\n"""

# Define un diccionario de opciones y las funciones asociadas
options = {
    "": iniciar_app,
    "setup": setup_app,
    "migrate": migrate,
    "stats": lambda: iniciar_app(mode="stats"),
    "downloadSPX": lambda: iniciar_app(mode="DownloadSPX"),
    "downloadStocks": lambda: iniciar_app(mode="DownloadStocks"),
    "updateStocks": lambda: iniciar_app(mode="UpdateStocks"),
    "updateSPX": lambda: iniciar_app(mode="UpdateSPX"),
    "companyOverview": lambda: iniciar_app(mode="companyOverview"),
    "IncomeStatement": lambda: iniciar_app(mode="IncomeStatement"),
    "BalanceSheet": lambda: iniciar_app(mode="BalanceSheet"),
    "CashFlow": lambda: iniciar_app(mode="CashFlow"),
    "Earnings": lambda: iniciar_app(mode="Earnings"),
    "compute": lambda: iniciar_app(mode="compute"),
    "full": lambda: iniciar_app(mode="computeAndResults"),
    "signals": lambda: iniciar_app(mode="lastResults"),
}


if __name__ == "__main__":
    # Get system arguments

    try:
        print("\033c")
        print(some_art)

        selected_option = input(main_text)
        print("")

        if selected_option in options:
            options[selected_option]()
        else:
            print(f"Valid options are: {list(options.keys())}")

    except Exception as e:
        logger.critical("Fatal error occurred: %s", str(e))
