from src.system.logging_config import logger
from .AlphaVantajeBase import AlphaVantajeBase

# Configure the logger



class CoreStockAPIs:
    @staticmethod
    def time_series_intraday(**parameters):
        """
        Fetches intraday time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_INTRADAY

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Required: interval
        Time interval between two consecutive data points in the time series.
        The following values are supported: 1min, 5min, 15min, 30min, 60min

        ❚ Optional: adjusted
        By default, adjusted=true and the output time series is adjusted by historical split and dividend events.
        Set adjusted=false to query raw (as-traded) intraday values.

        ❚ Optional: extended_hours
        By default, extended_hours=true and the output time series will include both the regular trading hours
        and the extended trading hours (4:00am to 8:00pm Eastern Time for the US market).
        Set extended_hours=false to query regular trading hours (9:30am to 4:00pm US Eastern Time) only.

        ❚ Optional: month
        By default, this parameter is not set and the API will return intraday data for the most recent days of trading.
        You can use the month parameter (in YYYY-MM format) to query a specific month in history.
        For example, month=2009-01. Any month in the last 20+ years since 2000-01 (January 2000) is supported.

        ❚ Optional: outputsize
        By default, outputsize=compact. Strings compact and full are accepted with the following specifications:
        compact returns only the latest 100 data points in the intraday time series;
        full returns trailing 30 days of the most recent intraday data if the month parameter (see above) is not specified,
        or the full intraday data for a specific month in history if the month parameter is specified.
        The "compact" option is recommended if you would like to reduce the data size of each API call.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the intraday time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "interval", "apikey"]
        optional = ["adjusted", "extended_hours", "month", "outputsize", "datatype","entitlement"]

        parameters["function"] = "TIME_SERIES_INTRADAY"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_daily(**parameters):
        """
        Fetches raw daily time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_DAILY

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: outputsize
        By default, outputsize=compact. Strings compact and full are accepted with the following specifications:
        compact returns only the latest 100 data points; full returns the full-length time series of 20+ years of historical data.
        The "compact" option is recommended if you would like to reduce the data size of each API call.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the daily time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["outputsize", "datatype","entitlement"]

        parameters["function"] = "TIME_SERIES_DAILY"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_daily_adjusted(**parameters):
        """
        Fetches raw daily open/high/low/close/volume values, adjusted close values, and historical split/dividend events
        for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_DAILY_ADJUSTED

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: outputsize
        By default, outputsize=compact. Strings compact and full are accepted with the following specifications:
        compact returns only the latest 100 data points; full returns the full-length time series of 20+ years of historical data.
        The "compact" option is recommended if you would like to reduce the data size of each API call.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the daily time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["outputsize", "datatype"]

        parameters["function"] = "TIME_SERIES_DAILY_ADJUSTED"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_weekly(**parameters):
        """
        Fetches weekly time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_WEEKLY

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the weekly time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["datatype","entitlement"]

        parameters["function"] = "TIME_SERIES_WEEKLY"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_weekly_adjusted(**parameters):
        """
        Fetches weekly adjusted time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_WEEKLY_ADJUSTED

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the weekly time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "TIME_SERIES_WEEKLY_ADJUSTED"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_monthly(**parameters):
        """
        Fetches monthly time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_MONTHLY

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the monthly time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["datatype","entitlement"]

        parameters["function"] = "TIME_SERIES_MONTHLY"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def time_series_monthly_adjusted(**parameters):
        """
        Fetches monthly adjusted time series data for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The time series of your choice. In this case, function=TIME_SERIES_MONTHLY_ADJUSTED

        ❚ Required: symbol
        The name of the equity of your choice. For example: symbol=IBM

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the monthly time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "TIME_SERIES_MONTHLY_ADJUSTED"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    def get_global_quote(**parameters):
        """
        Fetches the latest price and volume information for a given equity symbol.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The API function of your choice.

        ❚ Required: symbol
        The symbol of the global ticker of your choice. For example: symbol=IBM.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the quote data in JSON format; csv returns the quote data as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "symbol", "apikey"]
        optional = ["datatype"]

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def symbol_search(**parameters):
        """
        Search for stock symbols based on keywords.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The API function of your choice. In this case, function=SYMBOL_SEARCH.

        ❚ Required: keywords
        A text string of your choice. For example: keywords=microsoft.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the search results in JSON format; csv returns the search results as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "keywords", "apikey"]
        optional = ["datatype"]

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_market_status(**parameters):
        """
        Get the current market status (open vs. closed) of major trading venues for equities, forex, and cryptocurrencies.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The API function of your choice. In this case, function=MARKET_STATUS.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]

        return AlphaVantajeBase.api_request(required, [], **parameters)



