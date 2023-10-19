from src.system.logging_config import logger
from .TwelveDataBase import TwelveDataBase

# Configure the logger



class ReferenceData:
    @staticmethod
    def stocks_list(**parameters):
        """
        Retrieves an array of symbols available at the Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Optional: symbol
        - Filter by symbol.

        - Optional: exchange
        - Filter by exchange name or MIC code.

        - Optional: mic_code
        - Filter by market identifier code (MIC) under ISO 10383 standard.

        - Optional: country
        - Filter by country name or alpha code.

        - Optional: type
        - Filter by instrument type.

        - Optional: format
        - Data format (JSON or CSV, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        - Optional: show_plan
        - Adds info on which plan symbols are available (boolean, default false).

        - Optional: include_delisted
        - Include delisted identifiers (boolean, default false).

        Returns:
        - JSON array with symbols available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/stocks

        Example JSON Request with Symbol Filter:
        https://api.twelvedata.com/stocks?symbol=AAPL

        Example JSON Request with Plan Info:
        https://api.twelvedata.com/stocks?show_plan=true

        Example Downloadable CSV:
        https://api.twelvedata.com/stocks?format=CSV
        """
        endpoint = "/stocks"

        # Your function code here
        optional = [
            "symbol",
            "exchange",
            "mic_code",
            "country",
            "type",
            "format",
            "delimiter",
            "show_plan",
            "include_delisted",
        ]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def forex_pairs_list(**parameters):
        """
        Retrieves an array of forex pairs available at the Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Optional: symbol
        - Filter by symbol.

        - Optional: currency_base
        - Filter by currency base.

        - Optional: currency_quote
        - Filter by currency quote.

        - Optional: format
        - Data format (CSV or JSON, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        Returns:
        - JSON array with forex pairs available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/forex_pairs

        Example JSON Request with Symbol Filter:
        https://api.twelvedata.com/forex_pairs?symbol=EUR/USD

        Example Downloadable CSV:
        https://api.twelvedata.com/forex_pairs?format=CSV
        """
        endpoint = "/forex_pairs"
        # Your function code here
        optional = ["symbol", "currency_base", "currency_quote", "format", "delimiter"]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def cryptocurrencies_list(**parameters):
        """
        Retrieves an array of cryptocurrency pairs available at the Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Optional: symbol
        - Filter by symbol.

        - Optional: exchange
        - Filter by exchange name.

        - Optional: currency_base
        - Filter by currency base.

        - Optional: currency_quote
        - Filter by currency quote.

        - Optional: format
        - Data format (CSV or JSON, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        Returns:
        - JSON array with cryptocurrency pairs available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/cryptocurrencies

        Example JSON Request with Symbol Filter:
        https://api.twelvedata.com/cryptocurrencies?symbol=BTC/USD

        Example Downloadable CSV:
        https://api.twelvedata.com/cryptocurrencies?format=CSV
        """
        endpoint = "/cryptocurrencies"
        # Your function code here
        optional = [
            "symbol",
            "exchange",
            "currency_base",
            "currency_quote",
            "format",
            "delimiter",
        ]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def etf_list(**parameters):
        """
        Retrieves an array of ETFs available at the Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

            API Parameters:
            - Optional: symbol
            - Filter by symbol.

            - Optional: exchange
            - Filter by exchange name or MIC code.

            - Optional: mic_code
            - Filter by market identifier code (MIC) under ISO 10383 standard.

            - Optional: country
            - Filter by country name or alpha code.

            - Optional: format
            - Data format (CSV or JSON, default JSON).

            - Optional: delimiter
            - Specify the delimiter used when downloading the CSV file (default ';').

            - Optional: show_plan
            - Adds info on which plan symbol is available (boolean, default false).

            - Optional: include_delisted
            - Include delisted identifiers (boolean, default false).

        Returns:
        - JSON array with ETFs available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/etf

        Example JSON Request with Symbol Filter:
        https://api.twelvedata.com/etf?symbol=QQQ

        Example Downloadable CSV:
        https://api.twelvedata.com/etf?format=CSV
        """

        endpoint = "/etf"

        # Your function code here
        optional = [
            "symbol",
            "exchange",
            "mic_code",
            "country",
            "format",
            "delimiter",
            "show_plan",
            "include_delisted",
        ]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def exchanges(**parameters):
        """
        Retrieves an array of stock, ETF, or index exchanges available at Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Optional: type
        - The asset class to which the instrument belongs.
        - Supports: American Depositary Receipt, Bond, Bond Fund, Closed-end Fund, Common Stock, Depositary Receipt,
            Digital Currency, ETF, Exchange-Traded Note, Global Depositary Receipt, Index, Limited Partnership,
            Mutual Fund, Physical Currency, Preferred Stock, REIT, Right, Structured Product, Trust, Unit, and Warrant.

        - Optional: name
        - Filter by exchange name.

        - Optional: code
        - Filter by market identifier code (MIC) under ISO 10383 standard.

        - Optional: country
        - Filter by country name or alpha code.

        - Optional: format
        - Data format (CSV or JSON, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        - Optional: show_plan
        - Adds info on which plan the symbol is available (boolean, default false).

        Returns:
        - JSON array with stock, ETF, or index exchanges available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/exchanges

        Example JSON Request with Type Filter:
        https://api.twelvedata.com/exchanges?type=etf

        Example Downloadable CSV:
        https://api.twelvedata.com/exchanges?format=CSV
        """

        endpoint = "/exchanges"

        # Your function code here
        optional = [
            "type",
            "name",
            "code",
            "country",
            "format",
            "delimiter",
            "show_plan",
        ]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def cryptocurrency_exchanges(**parameters):
        """
        Retrieves an array of cryptocurrency exchanges available at Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Optional: format
        - Data format (CSV or JSON, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        Returns:
        - JSON array with cryptocurrency exchanges available at the Twelve Data API.

        Example JSON Request:
        https://api.twelvedata.com/cryptocurrency_exchanges

        Example Downloadable CSV:
        https://api.twelvedata.com/cryptocurrency_exchanges?format=CSV
        """

        endpoint = "/cryptocurrency_exchanges"

        # Your function code here
        optional = ["format", "delimiter"]

        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)

    @staticmethod
    def symbol_search(**parameters):
        """
        Find the best matching symbol to a given search query.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
        - The symbol to search for.

        - Optional: outputsize
        - Number of matches in response (default 30, max 120).

        - Optional: show_plan
        - Adds info on which plan the symbol is available (boolean, default false).

        Returns:
        - JSON array with matching symbols.

        Example JSON Request:
        https://api.twelvedata.com/symbol_search?symbol=AA

        Example JSON Request with show_plan:
        https://api.twelvedata.com/symbol_search?symbol=RY&show_plan=true
        """

        endpoint = "/symbol_search"

        # Your function code here
        required = ["symbol"]
        optional = ["outputsize", "show_plan"]

        return TwelveDataBase.api_request(endpoint, required, optional, **parameters)

    @staticmethod
    def earliest_timestamp(**parameters):
        """
        Get the earliest available timestamp for a given instrument at the specified interval.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict

        API Parameters:
        - Required: symbol, interval
        - symbol: Symbol ticker of the instrument.
        - interval: Interval between two consecutive points in time series (e.g., 1day, 1week).

        - Optional: exchange, mic_code, apikey, timezone

        Returns:
        - JSON with the earliest timestamp and its UNIX time.

        Example JSON Request:
        https://api.twelvedata.com/earliest_timestamp?symbol=AAPL&interval=1day&apikey=demo
        """

        endpoint = "/earliest_timestamp"

        # Your function code here
        required = ["symbol", "interval","apikey"]
        optional = ["exchange", "mic_code", "timezone"]

        return TwelveDataBase.api_request(endpoint, required, optional, **parameters)

    @staticmethod
    def get_indices(**parameters):
        """
        Get a list of indices available at Twelve Data API.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: list

        API Parameters (all optional):
        - symbol: Filter by symbol (e.g., "IXIC").
        - exchange: Filter by exchange name or mic code (e.g., "NASDAQ").
        - mic_code: Filter by market identifier code (MIC) under ISO 10383 standard.
        - country: Filter by country name or alpha code.
        - format: Value can be "CSV" or "JSON"; Default is JSON.
        - delimiter: Specify the delimiter used when downloading the CSV file; Default is semicolon ";".
        - show_plan: Adds info on which plan the symbol is available; Boolean; Default is false.
        - include_delisted: Include delisted identifiers; Boolean; Default is false.

        Example JSON Request:
        https://api.twelvedata.com/indices?symbol=IXIC&format=JSON

        Example CSV Request:
        https://api.twelvedata.com/indices?format=CSV
        """

        endpoint = "/indices"

        # Define the optional parameters
        optional = [
            "symbol",
            "exchange",
            "mic_code",
            "country",
            "format",
            "delimiter",
            "show_plan",
            "include_delisted",
        ]

        # Make the API request to Twelve Data
        return TwelveDataBase.api_request(endpoint, [], optional, **parameters)
