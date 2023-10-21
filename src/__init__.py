from .services import *
from src.services.DOWNLOAD import *

def main():
    logger.info('Starting application')
    # Schedule the task of downloading the SPX
    Scheduling.schedule_spx_download('10:00')
    # Schedule the task of updating the SPX
    Scheduling.schedule_spx_update()
    # Schedule the task of downloading stocks
    Scheduling.schedule_stock_download('15:11')
    # Schedule the task of updating stocks
    
    
    
    while True:
        Scheduling.checkForTask()
    