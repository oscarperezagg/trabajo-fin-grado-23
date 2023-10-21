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
    def schedule_spx_update():
        logger.info('Scheduling SPX update every 1 hour')
        schedule.every(1).hour.do(TDA_CoreData.updateAssets)
        logger.info('Scheduled SPX update every 1 hour')
    
    @staticmethod
    def schedule_stock_download(time):
        logger.info('Scheduling Stocks download for ' + time)
        schedule.every().day.at(time).do(AV_CoreData.downloadAsset)
        logger.info('Scheduled Stocks download for ' + time)
        
    @staticmethod 
    def checkForTask():
        schedule.run_pending()
        time.sleep(1)
    
    

