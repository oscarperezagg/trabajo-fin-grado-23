import pandas as pd
import pandas_ta as ta
import time

import talib
from src.lib.MONGODB import *
from src.system.secret import *
from datetime import datetime, timedelta
import numpy as np


class parameters:
    ###############################################################
    ##################### FORMAT DATA  ############################
    ###############################################################

    @staticmethod
    def formatData(data):
        data = pd.DataFrame(data["data"])

        # Convertir las columnas 'open', 'high', 'low', 'close' y 'volume' a tipo float
        columns_to_convert = ["open", "high", "low", "close", "volume"]
        data[columns_to_convert] = data[columns_to_convert].astype(float)

        # Convertir la columna 'datetime' a tipo datetime
        data["datetime"] = pd.to_datetime(data["datetime"])
        data["date"] = data["datetime"]
        # Ordenar el DataFrame por la columna 'datetime' en orden ascendente
        data.sort_values(by="datetime", inplace=True)

        data.set_index(pd.DatetimeIndex(data["datetime"]), inplace=True)

        # Eliminar la columna 'datetime'
        data.drop(columns=["datetime"], inplace=True)

        return data

    @staticmethod
    def matchDates(data, spx):
        """
        Esta función elimina los datos que no se encuentran en las fechas
        especificadas
        """

        # Si al fecha de spx es mayor
        if spx.iloc[0]["date"] > data.iloc[0]["date"]:
            return data[(data["date"] >= spx.iloc[0]["date"])]
        else:
            return data

    #########################################################
    ######################  SMA  ############################
    #########################################################

    @staticmethod
    def simplemovingaverage(data, period):
        # Calcular el SMA de 10 períodos utilizando la función talib.SMA
        sma = talib.SMA(data["close"], timeperiod=period)
        data[f"SMA_{period}"] = sma

    #########################################################
    ######################  EMA  ############################
    #########################################################

    @staticmethod
    def exponentialmovingaverage(data, period):
        # Calcular el SMA de 10 períodos utilizando la función talib.SMA
        sma = talib.EMA(data["close"], timeperiod=period)
        data[f"EMA_{period}"] = sma

    #########################################################
    ######################  RSI  ############################
    #########################################################

    @staticmethod
    def relativestregthindex(data, period):
        # Calcular el SMA de 10 períodos utilizando la función talib.SMA
        rsi = talib.RSI(data["close"], timeperiod=period)
        data[f"RSI_{period}"] = rsi

    ##########################################################
    ######################  BETA  ############################
    ##########################################################

    def beta(stock, spx, testing=False):
        """
        Calcula el beta de un activo con respecto al SPX siempre
        con el intervalo de 1 día.
        """
        # Determinar la fecha inicial basada en si estamos en modo de prueba o no
        date = pd.Timestamp(TESTING_DATE if testing else NORMAL_DATE)

        # Sumar 5 años a la fecha 'date'
        fecha_menos_cinco_anios = date - pd.DateOffset(years=5)

        # Obtener la primera fecha del dataframe stock
        first_date_in_stock = stock.iloc[0]["date"]

        if first_date_in_stock > fecha_menos_cinco_anios:
            # La fecha tope es la primera fecha + 5 años
            date = first_date_in_stock + pd.DateOffset(years=5)

        # Aplicar la función solo a las filas filtradas
        stock["beta"] = stock.apply(
            parameters.all_betas, args=(date, spx, stock), axis=1
        )

    def all_betas(row, date, spx, data):
        if row["date"] < date:
            return np.nan

        data = data.copy()
        spx = spx.copy()

        # Asegúrate de que ambos DataFrames tienen suficientes datos
        if data.empty or spx.empty or data.shape[0] < 2 or spx.shape[0] < 2:
            return 0

        fecha_maxima = row["date"]

        # Restamos 5 años a la fecha actual
        fecha_hace_5_anios = row["date"] - timedelta(
            days=5 * 365 + 1
        )  # Añadimos un día adicional por el año bisiesto

        # Formatear la fecha al formato deseado (año-mes-día)
        fecha_minima = fecha_hace_5_anios.strftime("%Y-%m-%d")

        # Filtra el DataFrame utilizando .loc[]
        data = data[(data["date"] >= fecha_minima) & (data["date"] <= fecha_maxima)]
        spx = spx[(spx["date"] >= fecha_minima) & (spx["date"] <= fecha_maxima)]

        # Acotamos los dataframes a las fechas más antiguas entras las dos últimas
        spx_last_date = spx.iloc[0]["date"]
        data_last_date = data.iloc[0]["date"]

        if data_last_date > spx_last_date:
            spx = spx[(spx["date"] >= data_last_date)]
        elif spx_last_date > data_last_date:
            data = data[(data["date"] >= spx_last_date)]

        data["Retorno"] = data["close"].pct_change()
        spx["Retorno"] = spx["close"].pct_change()

        # Asegúrate de que la varianza del mercado no sea cero

        covarianza = data["Retorno"].cov(spx["Retorno"])
        varianza_mercado = spx["Retorno"].var()
        if varianza_mercado == 0:
            return 0
        beta = covarianza / varianza_mercado

        return beta

    def apply_beta(data, betas):
        # Luego resta un día para obtener el día anterior
        data["date_normalized"] = data["date"].dt.normalize() - pd.Timedelta(days=1)

        # Utilizar merge_asof para encontrar la fila en 'betas' que tenga la fecha justo anterior
        # a cada entrada en 'data_normalized' de 'data'
        merged_data = pd.merge_asof(
            data,
            betas,
            left_on="date_normalized",
            right_on="date",
            direction="backward",
        )
        merged_data.set_index("date_x", inplace=True)
        # Crear la nueva columna 'beta' en 'data' con los valores correspondientes
        data["beta"] = merged_data["beta"]

    ##############################################################
    ######################  Earnings  ############################
    ##############################################################

    def asignReportPresentationDate(symbol, data):
        """
        Está función crea una columna extra con la fecha y el tipo del último
        reporte
        """
        dates = parameters.getReportDates(symbol)

        data["lastReport"] = data.apply(
            parameters.privateAsignPresentationDate, args=(dates,), axis=1
        )

    def asignMovement(symbol, data):
        dates = parameters.getReportDates(symbol)
        results = {}
        for date in dates:
            # Comprobar si la fecha está en la columna 'date' de data
            if data["date"].isin([date]).any():
                # Calcular el cambio porcentual solo si la fecha está presente
                row = data[data["date"] == date]

                results[date] = ((row["close"] - row["open"]) / row["open"] * 100).iloc[
                    -1
                ]
            else:
                # Opcionalmente, puedes asignar un valor por defecto si la fecha no está presente
                results[date] = np.nan

        data["reportPriceMovement"] = data["lastReport"].map(results)

    ##### SUPORT FUNCTIONS FOR EARNING #####

    def privateAsignPresentationDate(row, dates):
        """
        Para cada fecha de los datos de intervalo 15 minutos,
        encuentra la última fecha de presentación de reportes
        """
        reference_datetime = row["date"]

        for date_str in dates:
            date = datetime.strptime(date_str, "%Y-%m-%d")

            # Comparar con la fecha de referencia
            if date < reference_datetime:
                return date_str

        return np.nan

    def getReportDates(symbol):
        """
        Obtiene las fechas de presentación de reportes de un activo
        en concreto. Esta función tiene una importanca muy grande,
        pues modifica las fechas. Esta modificación se debe a que las
        presentaciones puede encontrase en días que no son hábiles,
        y por lo tanto no hay movimiento de precios.
        """
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "CoreData",
        )
        data = conn.findByMultipleFields(
            fields={"symbol": symbol, "interval": "1day"}, custom=True
        )

        data = parameters.formatData(data)

        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "IncomeStatement",
        )

        incomeSheet = conn.findByMultipleFields(fields={"symbol": symbol}, custom=True)
        dates = [
            report["fiscalDateEnding"] for report in incomeSheet["quarterlyReports"]
        ]

        adjusted_dates = []

        for date_str in dates:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            original_date = date_obj
            found = False

            for _ in range(6):  # Incluye el día original más 5 días adicionales
                if data["date"].isin([date_obj]).any():
                    found = True
                    break
                date_obj += timedelta(days=1)

            adjusted_dates.append(
                date_obj.strftime("%Y-%m-%d")
                if found
                else original_date.strftime("%Y-%m-%d")
            )

        return adjusted_dates

