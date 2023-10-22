import schedule
import time
from src.modules.DATA_MODULE import *
from src.system.logging_config import logger


class Scheduling:
    @staticmethod
    def schedule_spx_download(time):
        logger.info("Scheduling SPX download for " + time)
        schedule.every().day.at(time).do(TDA_CoreData.downloadAsset)
        logger.info("Scheduled SPX download for " + time)

    @staticmethod
    def schedule_spx_update(option="hour", time=1):
        logger.info(f"Scheduling SPX update every {time} {option}")
        if option == "hour":
            schedule.every(time).hour.do(TDA_CoreData.updateAssets)
        elif option == "minute":
            schedule.every(time).minutes.do(TDA_CoreData.updateAssets)
        logger.info(f"Scheduled SPX update every {time} {option}")

    @staticmethod
    def schedule_stock_download(time):
        logger.info("Scheduling Stocks download for " + time)
        schedule.every().day.at(time).do(AV_CoreData.downloadAsset)
        logger.info("Scheduled Stocks download for " + time)

    @staticmethod
    def schedule_stocks_update(option="hour", time=1):
        logger.info(f"Scheduling SPX update every {time} {option}")
        if option == "hour":
            schedule.every(time).hour.do(AV_CoreData.updateAssets)
        elif option == "minute":
            schedule.every(time).minutes.do(TDA_CoreData.updateAssets)
        logger.info(f"Scheduled SPX update every {time} {option}")

    @staticmethod
    def schedule_stats(option="minute", time=5):
        logger.info(f"Scheduling stats every {time} {option}")
        if option == "hour":
            schedule.every(time).hour.do(Stats.get_stats)
        elif option == "minute":
            schedule.every(time).minutes.do(Stats.get_stats)
        elif option == "seconds":
            schedule.every(time).seconds.do(Stats.get_stats)
        logger.info(f"Scheduled stats every {time} {option}")

    @staticmethod
    def checkForTask():
        schedule.run_pending()
        time.sleep(1)
