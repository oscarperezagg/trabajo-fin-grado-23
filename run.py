import sys
from src import iniciar_app
from src.system.logging_config import logger

options = ["stats", "downloadSPX", "downloadStocks", "updateStocks", "updateSPX"]

if __name__ == "__main__":
    # Get system arguments
    
    args = sys.argv
    iniciar_app(DownloadStocks=True)
    if False:
        if len(args) == 1:
            iniciar_app()
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