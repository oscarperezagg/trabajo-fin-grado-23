from .services import *
from src.services.DOWNLOAD import *

def main():
    logger.info('Starting application')
    # Schedule the task of downloading the SPX
    Scheduling.schedule_spx_download('10:00')
    # Schedule the task of updating the SPX
    Scheduling.schedule_spx_update('12:00')
    
    
    while True:
        Scheduling.checkForTask()
    