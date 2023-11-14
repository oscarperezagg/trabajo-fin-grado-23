import sys
from src import iniciar_app, setup_app
from src.system.logging_config import logger

options = ["stats", "downloadSPX", "downloadStocks", "updateStocks", "updateSPX"]

some_art = """ _____ _____ ____   ____  _____      ____  _  _   
|_   _|  ___/ ___| |___ \|___ /     |___ \| || |  
  | | | |_ | |  _    __) | |_ \ _____ __) | || |_ 
  | | |  _|| |_| |  / __/ ___) |_____/ __/|__   _|
  |_| |_|   \____| |_____|____/     |_____|  |_|  
  
  
  Made by: Óscar Pérez Arruti\n\n"""

if __name__ == "__main__":
    # Get system arguments
    print("\033c")

    print(some_art)

    args = sys.argv

    if len(args) == 1:
        iniciar_app()
    elif args[1] == "setup":
        setup_app()
    elif args[1] == "stats":
        iniciar_app(stats=True)
    elif args[1] == "downloadSPX":
        iniciar_app(DownloadSPX=True)
    elif args[1] == "downloadStocks":
        iniciar_app(DownloadStocks=True)
    elif args[1] == "updateStocks":
        iniciar_app(UpdateStocks=True)
    elif args[1] == "updateSPX":
        iniciar_app(UpdateSPX=True)
    else:
        print(f"Valid options are: {options}")
