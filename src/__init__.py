from src.services import *
from src.services.DOWNLOAD import *

def main(justStats=False):
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
        Scheduling.schedule_stock_download('21:26')
        # Schedule the task of updating stocks
        # Scheduling.schedule_stocks_update('hour', 1)
        
    Scheduling.schedule_stats('minute', 1)

    while True:
        Scheduling.checkForTask()
        