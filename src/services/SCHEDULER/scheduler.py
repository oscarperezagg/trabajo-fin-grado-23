import schedule
import time
from src.services.DOWNLOAD import *
from src.system.logging_config import logger
    
class Scheduling:
    
    @staticmethod
    def schedule_spx_download(time):
        logger.info('Scheduling SPX download for ' + time)
        schedule.every().day.at(time).do(TDA_CoreData.downloadAsset)
        logger.info('Scheduled SPX download for ' + time)
        
    @staticmethod
    def schedule_spx_update(time):
        logger.info('Scheduling SPX update for ' + time)
        schedule.every().day.at(time).do(TDA_CoreData.updateAssets)
        logger.info('Scheduled SPX update for ' + time)
        
    @staticmethod 
    def checkForTask():
        schedule.run_pending()
        time.sleep(1)
    
    

