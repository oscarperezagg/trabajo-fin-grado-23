from src.system.logging_config import logger
from .AlphaVantajeBase import AlphaVantajeBase

# Configure the logger



class EconomicCalendar:
    @staticmethod
    def get_real_gdp(**parameters):
        """
        Fetches the annual or quarterly Real GDP of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=REAL_GDP

        ❚ Optional: interval
        By default, interval=annual. Strings quarterly and annual are accepted.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["interval", "datatype"]

        parameters["function"] = "REAL_GDP"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_real_gdp_per_capita(**parameters):
        """
        Fetches the quarterly Real GDP per Capita data of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=REAL_GDP_PER_CAPITA

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "REAL_GDP_PER_CAPITA"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_treasury_yield(**parameters):
        """
        Fetches the daily, weekly, or monthly US treasury yield of a given maturity timeline (e.g., 5 year, 30 year, etc).

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=TREASURY_YIELD

        ❚ Optional: interval
        By default, interval=monthly. Strings daily, weekly, and monthly are accepted.

        ❚ Optional: maturity
        By default, maturity=10year. Strings 3month, 2year, 5year, 7year, 10year, and 30year are accepted.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["interval", "maturity", "datatype"]

        parameters["function"] = "TREASURY_YIELD"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_federal_funds_rate(**parameters):
        """
        Fetches the daily, weekly, or monthly federal funds rate (interest rate) of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=FEDERAL_FUNDS_RATE

        ❚ Optional: interval
        By default, interval=monthly. Strings daily, weekly, and monthly are accepted.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["interval", "datatype"]

        parameters["function"] = "FEDERAL_FUNDS_RATE"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_cpi(**parameters):
        """
        Fetches the monthly and semiannual consumer price index (CPI) of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=CPI

        ❚ Optional: interval
        By default, interval=monthly. Strings monthly and semiannual are accepted.

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["interval", "datatype"]

        parameters["function"] = "CPI"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_inflation(**parameters):
        """
        Fetches the annual inflation rates (consumer prices) of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=INFLATION

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "INFLATION"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_retail_sales(**parameters):
        """
        Fetches the monthly Advance Retail Sales: Retail Trade data of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=RETAIL_SALES

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "RETAIL_SALES"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_durable_goods_orders(**parameters):
        """
        Fetches the monthly Manufacturers' New Orders: Durable Goods data of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=DURABLES

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "DURABLES"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_unemployment_rate(**parameters):
        """
        Fetches the monthly Unemployment Rate data of the United States.

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=UNEMPLOYMENT

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "UNEMPLOYMENT"

        return AlphaVantajeBase.api_request(required, optional, **parameters)

    @staticmethod
    def get_nonfarm_payroll(**parameters):
        """
        Fetches the monthly US All Employees: Total Nonfarm (commonly known as Total Nonfarm Payroll).

        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        ❚ Required: function
        The function of your choice. In this case, function=NONFARM_PAYROLL

        ❚ Optional: datatype
        By default, datatype=json. Strings json and csv are accepted with the following specifications:
        json returns the time series in JSON format; csv returns the time series as a CSV (comma separated value) file.

        ❚ Required: apikey
        Your API key. Claim your free API key here.
        """
        required = ["function", "apikey"]
        optional = ["datatype"]

        parameters["function"] = "NONFARM_PAYROLL"

        return AlphaVantajeBase.api_request(required, optional, **parameters)
