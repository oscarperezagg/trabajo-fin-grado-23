from src.system.logging_config import logger
from .TwelveDataBase import TwelveDataBase

# Configure the logger



class CoreData:
    @staticmethod
    def time_series_intraday(**parameters):
        """
        Obtiene datos de series temporales intradía para un instrumento financiero específico.

        Parámetros requeridos:
        - symbol (string): Ticker del instrumento.
        - interval (string): Intervalo entre dos puntos consecutivos en la serie temporal.
                            Puede ser uno de los siguientes valores: 1min, 5min, 15min, 30min, 45min,
                            1h, 2h, 4h, 1day, 1week, 1month.

        Parámetros opcionales:
        - exchange (string): Bolsa en la que se negocia el instrumento.
        - mic_code (string): Código de Identificación de Mercado (MIC) bajo el estándar ISO 10383.
        - country (string): País en el que se negocia el instrumento.
        - type (string): Clase de activo a la que pertenece el instrumento. Puede ser uno de los siguientes:
                        American Depositary Receipt, Bond, Bond Fund, Closed-end Fund, Common Stock,
                        Depositary Receipt, Digital Currency, ETF, Exchange-Traded Note, Global Depositary Receipt,
                        Index, Limited Partnership, Mutual Fund, Physical Currency, Preferred Stock, REIT, Right,
                        Structured Product, Trust, Unit, Warrant.
        - outputsize (number): Número de puntos de datos a recuperar (rango de 1 a 5000, predeterminado 30 si no se especifican parámetros de fecha).
        - format (string): Formato de salida de los datos (JSON o CSV, predeterminado JSON).
        - delimiter (string): Delimitador utilizado al descargar el archivo CSV (predeterminado punto y coma ';').
        - prepost (string): Disponible en intervalos de 1min, 5min, 15min y 30min para todas las acciones estadounidenses.
                        Abre, alto, bajo y cierra los valores se suministran sin volumen.
        - dp (string): Parámetro avanzado. Punto de datos anterior utilizado para calcular las medias móviles y otros indicadores.
        - order (string): Parámetro avanzado. Orden de los resultados (ascendente o descendente).
        - timezone (string): Parámetro avanzado. Zona horaria deseada para la serie temporal.
        - date (string): Parámetro avanzado. Fecha específica para la cual se desean los datos.
        - start_date (string): Parámetro avanzado. Fecha de inicio para el rango de datos.
        - end_date (string): Parámetro avanzado. Fecha de finalización para el rango de datos.
        - previous_close (string): Parámetro avanzado. Incluye el precio de cierre anterior en los datos.

        Retorna:
        - Un objeto JSON con los datos de la serie temporal o un archivo CSV descargable, según el formato especificado.
        - En caso de éxito, la respuesta contiene información general del instrumento y una lista de puntos de datos con los campos datetime, open, high, low, close y volume.

        Ejemplo de solicitud JSON:
        https://api.twelvedata.com/time_series?symbol=AAPL&interval=1min&apikey=demo

        Ejemplo de solicitud JSON con parámetros avanzados:
        https://api.twelvedata.com/time_series?symbol=EUR/USD&interval=1day&outputsize=12&apikey=demo

        Ejemplo de solicitud CSV descargable:
        https://api.twelvedata.com/time_series?symbol=BTC/USD&interval=5min&format=CSV&apikey=demo
        """
        endpoint = "/time_series"
        
        # Parámetros requeridos
        required = ["apikey","symbol", "interval"]

        # Parámetros opcionales
        optional = [
            "exchange",
            "mic_code",
            "country",
            "type",
            "outputsize",
            "format",
            "delimiter",
            "prepost",
            "dp",
            "order",
            "timezone",
            "date",
            "start_date",
            "end_date",
            "previous_close",
        ]

        return TwelveDataBase.api_request(endpoint,required, optional, **parameters)

    @staticmethod
    def exchange_rate(**parameters):
        """
        Retrieves real-time exchange rate for a specified currency pair.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
        - The currency pair in the format 'BaseCurrency/QuoteCurrency' (e.g., USD/JPY, BTC/ETH).

        - Optional: date
        - If provided, it uses the exchange rate from a specific date or time.
        - Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.

        - Optional: format
        - Data format (JSON or CSV, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        - Required: apikey
        - Your API key obtained from here.

        Advanced Parameters (Optional):
        - None

        Returns:
        - JSON object with information about the requested currency pair.
        - Includes symbol (currency pair), rate (real-time exchange rate), and timestamp (Unix timestamp).

        Example JSON Request:
        https://api.twelvedata.com/exchange_rate?symbol=USD/JPY&apikey=demo

        Example JSON Request with Date:
        https://api.twelvedata.com/exchange_rate?symbol=USD/JPY&date=2022-02-22&apikey=demo

        Example Downloadable CSV:
        https://api.twelvedata.com/exchange_rate?symbol=EUR/USD&format=CSV&apikey=demo
        """
        # Your function code here
        required = ["apikey","symbol", "apikey"]

        # Lista de parámetros opcionales
        optional = ["date", "format", "delimiter"]

        return TwelveDataBase.api_request(required, optional, **parameters)

    @staticmethod
    def currency_conversion(**parameters):
        """
        Retrieves real-time exchange rate and converted amount for a specified currency pair.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
            - The currency pair in the format 'BaseCurrency/QuoteCurrency' (e.g., USD/JPY, BTC/ETH).

        - Required: amount
                - Amount of base currency to be converted into quote currency.
                - Supports values from 0 and above.

        - Optional: date
            - If provided, it uses the exchange rate from a specific date or time.
            - Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.

        - Optional: format
            - Data format (JSON or CSV, default JSON).

        - Optional: delimiter
            - Specify the delimiter used when downloading the CSV file (default ';').

        - Required: apikey
            - Your API key obtained from here.

        Advanced Parameters (Optional):
        - None

        Returns:
        - JSON object with information about the requested currency pair.
        - Includes symbol (currency pair), rate (real-time exchange rate), converted amount, and timestamp (Unix timestamp).

        Example JSON Request:
        https://api.twelvedata.com/currency_conversion?symbol=USD/JPY&amount=122&apikey=demo

        Example JSON Request with Date:
        https://api.twelvedata.com/currency_conversion?symbol=USD/JPY&amount=122&date=2022-02-22&apikey=demo

        Example Downloadable CSV:
        https://api.twelvedata.com/currency_conversion?symbol=EUR/USD&amount=15&format=CSV&apikey=demo
        """
        # Your function code here
        required = ["apikey","symbol", "amount", "apikey"]

        # Lista de parámetros opcionales
        optional = ["date", "format", "delimiter"]

        return TwelveDataBase.api_request(required, optional, **parameters)

    @staticmethod
    def quote(**parameters):
        """
        Retrieves the latest quote of a specified instrument.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
        - Symbol ticker of the instrument (e.g., AAPL, EUR/USD, ETH/BTC).

        - Optional: interval
        - Interval of the quote.
        - Supports: 1min, 5min, 15min, 30min, 45min, 1h, 2h, 4h, 1day, 1week, 1month; Default 1day.

        - Optional: exchange
        - Exchange where the instrument is traded.

        - Optional: mic_code
        - Market Identifier Code (MIC) under ISO 10383 standard.

        - Optional: country
        - Country where the instrument is traded.

        - Optional: volume_time_period
        - Number of periods for Average Volume; Default 9.

        - Optional: type
        - The asset class to which the instrument belongs.

        - Optional: format
        - Data format (JSON or CSV, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        - Required: apikey
        - Your API key obtained from here.

        - Optional: prepost
        - Available at the 1min, 5min, 15min, and 30min intervals for all US equities.
        - Open, high, low, close values are supplied without volume; Default false.

        - Optional: eod
        - If true, then return data for the closed day.

        - Optional: rolling_period
        - Number of hours to calculate rolling change at the period. Default is 24; can be in range [1, 168].

        Advanced Parameters (Optional):
        - None

        Returns:
        - JSON object with information about the requested instrument.
        - Includes symbol, name, exchange, mic_code, currency, timestamp, datetime, open, high, low, close, volume,
        previous_close, change, percent_change, average_volume, rolling_1d_change, rolling_7d_change, rolling_period_change,
        is_market_open, fifty_two_week, extended_change, extended_percent_change, extended_price, and extended_timestamp.

        Example JSON Request:
        https://api.twelvedata.com/quote?symbol=AAPL&apikey=demo

        Example JSON Request with Interval:
        https://api.twelvedata.com/quote?symbol=EUR/USD&interval=30min&apikey=demo

        Example Downloadable CSV:
        https://api.twelvedata.com/quote?symbol=IXIC&type=Index&interval=5min&format=CSV&apikey=demo
        """
        # Your function code here
        required = ["apikey","symbol", "apikey"]

        # Lista de parámetros opcionales
        optional = [
            "interval",
            "exchange",
            "mic_code",
            "country",
            "volume_time_period",
            "type",
            "format",
            "delimiter",
            "prepost",
            "eod",
            "rolling_period",
        ]

        return TwelveDataBase.api_request(required, optional, **parameters)

    @staticmethod
    def price(**parameters):
        """
        Retrieves the real-time price of a specified instrument.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
        - Symbol ticker of the instrument (e.g., AAPL, EUR/USD, ETH/BTC).

        - Optional: exchange
        - Exchange where the instrument is traded.

        - Optional: mic_code
        - Market Identifier Code (MIC) under ISO 10383 standard.

        - Optional: country
        - Country where the instrument is traded.

        - Optional: type
        - The asset class to which the instrument belongs.

        - Optional: format
        - Data format (JSON or CSV, default JSON).

        - Optional: delimiter
        - Specify the delimiter used when downloading the CSV file (default ';').

        - Required: apikey
        - Your API key obtained from here.

        - Optional: prepost
        - Available at the 1min, 5min, 15min, and 30min intervals for all US equities.
        - Open, high, low, close values are supplied without volume; Default false.

        - Optional: dp
        - Specifies the number of decimal places for floating values; Should be in range [0,11] inclusive; Default 5.

        Returns:
        - JSON object with the real-time or latest available price of the requested instrument.

        Example JSON Request:
        https://api.twelvedata.com/price?symbol=AAPL&apikey=demo

        Example JSON Request with Country:
        https://api.twelvedata.com/price?symbol=TRP&country=Canada&apikey=demo

        Example Downloadable CSV:
        https://api.twelvedata.com/price?symbol=USD/JPY&format=CSV&apikey=demo
        """
        # Your function code here
        required = ["apikey","symbol", "apikey"]

        # Lista de parámetros opcionales
        optional = [
            "exchange",
            "mic_code",
            "country",
            "type",
            "format",
            "delimiter",
            "prepost",
            "dp",
        ]

        return TwelveDataBase.api_request(required, optional, **parameters)

    @staticmethod
    def end_of_day_price(**parameters):
        """
        Retrieves the latest End of Day (EOD) price of a specified instrument.

        Parameters:
        :param parameters: A dictionary containing API parameters.
        :type parameters: dict

        Returns:
        :return: API response data.
        :rtype: dict or tuple

        API Parameters:
        - Required: symbol
        - Symbol ticker of the instrument (e.g., AAPL, EUR/USD, ETH/BTC).

        - Optional: exchange
        - Exchange where the instrument is traded.

        - Optional: mic_code
        - Market Identifier Code (MIC) under ISO 10383 standard.

        - Optional: country
        - Country where the instrument is traded.

        - Optional: type
        - The asset class to which the instrument belongs.

        - Required: apikey
        - Your API key obtained from here.

        - Optional: prepost
        - Available at the 1min, 5min, 15min, and 30min intervals for all US equities.
        - Open, high, low, close values are supplied without volume; Default false.

        - Optional: dp
        - Specifies the number of decimal places for floating values; Should be in range [0,11] inclusive; Default 5.

        Returns:
        - JSON object with the latest End of Day (EOD) price of the requested instrument.

        Example JSON Request:
        https://api.twelvedata.com/eod?symbol=AAPL&apikey=demo

        Example JSON Request with Country:
        https://api.twelvedata.com/eod?symbol=TRP&country=Canada&apikey=demo
        """
        # Your function code here
        required = ["apikey","symbol", "apikey"]

        # Lista de parámetros opcionales
        optional = ["exchange", "mic_code", "country", "type", "prepost", "dp"]

        return TwelveDataBase.api_request(required, optional, **parameters)
