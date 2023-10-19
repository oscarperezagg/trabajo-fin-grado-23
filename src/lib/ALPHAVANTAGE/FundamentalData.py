from src.system.logging_config import logger
from .AlphaVantajeBase import AlphaVantajeBase

# Configure the logger



class FundamentalData:
    @staticmethod
    def get_company_overview(**parameters):
        """
        Fetches company overview information for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=OVERVIEW

        ❚ Required: symbol
        The symbol of the ticker of your choice. For example: symbol=IBM.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = []

        parameters["function"] = "OVERVIEW"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_income_statement(**parameters):
        """
        Fetches annual and quarterly income statements for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=INCOME_STATEMENT

        ❚ Required: symbol
        The symbol of the ticker of your choice. For example: symbol=IBM.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = []

        parameters["function"] = "INCOME_STATEMENT"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_balance_sheet(**parameters):
        """
        Fetches annual and quarterly balance sheet data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=BALANCE_SHEET

        ❚ Required: symbol
        The symbol of the ticker of your choice. For example: symbol=IBM.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = []

        parameters["function"] = "BALANCE_SHEET"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_cash_flow(**parameters):
        """
        Fetches annual and quarterly cash flow data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=CASH_FLOW

        ❚ Required: symbol
        The symbol of the ticker of your choice. For example: symbol=IBM.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = []

        parameters["function"] = "CASH_FLOW"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_earnings(**parameters):
        """
        Fetches annual and quarterly earnings (EPS) data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=EARNINGS

        ❚ Required: symbol
        The symbol of the ticker of your choice. For example: symbol=IBM.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = []

        parameters["function"] = "EARNINGS"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_listing_status(**parameters):
        """
        Fetches a list of active or delisted US stocks and ETFs either as of the latest trading day or at a specific time in history.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=LISTING_STATUS

        ❚ Optional: date
        If no date is set, the API endpoint will return a list of active or delisted symbols as of the latest trading day. If a date is set, the API endpoint will "travel back" in time and return a list of active or delisted symbols on that particular date in history. Any YYYY-MM-DD date later than 2010-01-01 is supported. For example, date=2013-08-03

        ❚ Optional: state
        By default, state=active and the API will return a list of actively traded stocks and ETFs. Set state=delisted to query a list of delisted assets.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["date", "state"]

        parameters["function"] = "LISTING_STATUS"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_earnings_calendar(**parameters):
        """
        Fetches a list of company earnings expected in the next 3, 6, or 12 months.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=EARNINGS_CALENDAR

        ❚ Optional: symbol
        By default, no symbol will be set for this API. When no symbol is set, the API endpoint will return the full list of company earnings scheduled. If a symbol is set, the API endpoint will return the expected earnings for that specific symbol. For example, symbol=IBM

        ❚ Optional: horizon
        By default, horizon=3month and the API will return a list of expected company earnings in the next 3 months. You may set horizon=6month or horizon=12month to query the earnings scheduled for the next 6 months or 12 months, respectively.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["symbol", "horizon"]

        parameters["function"] = "EARNINGS_CALENDAR"

        return AlphaVantajeBase.api_request(required, optional, **parameters)
