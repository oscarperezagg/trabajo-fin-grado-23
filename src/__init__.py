from .services import *
from src.services.DOWNLOAD import *

def main():
    TDA_CoreData.updateAssets()
    logger.info('Starting application')
    Scheduling.schedule_spx_download('12:00')
    
    
    while True:
        Scheduling.checkForTask()
    