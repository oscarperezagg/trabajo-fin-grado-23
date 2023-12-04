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
    def schedule_stock_download(option="hour", time=1):
        logger.info(f"Scheduling Stocks download every {time} {option}")
        if option == "hour":
            schedule.every(time).hour.do(AV_CoreData.downloadAsset)
        elif option == "minute":
            schedule.every(time).minutes.do(AV_CoreData.downloadAsset)
        logger.info(f"Scheduled Stocks download {time} {option}")

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
    def schedule_download_fundamental_data(time):
        logger.info("Scheduling Fundamental Data donwload for " + time)
        schedule.every().day.at(time).do(AV_FundamentalData.all_methods)
        logger.info("Scheduled Fundamental Data donwload for " + time)

    @staticmethod
    def checkForTask():
        schedule.run_pending()
        time.sleep(1)
