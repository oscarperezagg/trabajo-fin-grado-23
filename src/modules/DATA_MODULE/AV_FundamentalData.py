from datetime import datetime, timedelta
import time
from src.lib import *
from src.system.secret import *
from src.system.logging_config import logger
from requests import Response
from .CleanDatabase import *

from .util import *

# Configure the logger

trading_hours = {
    "start": "15:30",
    "end": "23:00",
}

LIMIT = 2020


class AV_FundamentalData:
    def getCompanyOverview():
        """
        Get the company overview from the API
        """
        assets = getConfig(api_name="alphavantage")["assets"]

        logger.info("Getting company overview")

        # Get the company overview for each asset
        for asset in assets:
            try:
                logger.info(f"Getting company overview for {asset}")
                # Get the company overview
                params = {
                    "symbol": asset,
                    "apikey": ALPHA_VANTAGE_API_KEY,
                }
                response = FundamentalData.get_company_overview(**params)
                # Check if the response is valid
                if response[0]:
                    # Insert the data into the database
                    conn = MongoDbFunctions(
                        DATABASE["host"],
                        DATABASE["port"],
                        DATABASE["username"],
                        DATABASE["password"],
                        DATABASE["dbname"],
                        "CompanyOverview",
                    )
                    data = response[1].json()
                    if data != {}:
                        del data["Symbol"]
                        data["symbol"] = asset
                        conn.deleteByField("symbol", asset)
                        conn.insert(data)
                    else:
                        logger.error(f"No company overview found for {asset}")
                else:
                    logger.error("An error occurred: %s", response[1])
            except Exception as e:
                logger.error(f"An error occurred: {str(e)}")
                continue
