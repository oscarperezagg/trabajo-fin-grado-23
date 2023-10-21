from src.services import *
from src.services.DOWNLOAD import *

def main():
    # Show stats
    Stats.get_stats()
    #AV_CoreData.downloadAsset()
    logger.info('Starting application')
    # Schedule the task of downloading the SPX
    Scheduling.schedule_spx_download('10:00')
    # Schedule the task of updating the SPX
    Scheduling.schedule_spx_update('hour', 1)
    # Schedule the task of downloading stocks
    Scheduling.schedule_stock_download('17:02')
    # Schedule the task of updating stocks
    Scheduling.schedule_stats('minute', 5)
    
    
    
    while True:
        Scheduling.checkForTask()
    