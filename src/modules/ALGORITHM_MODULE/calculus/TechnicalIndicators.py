import pandas as pd
from src.lib import MongoDbFunctions
from .formatter import CalculusFormater
from src.system.secret import *

class TechnicalIndicators:
    
    
    @staticmethod
    def simpleMovingAverage(data, period):
        """
        Simple Moving Average
        """
        # Calculate SMA
        print(f'Calculating SMA{period}...')
        print(data.head()   )
        data[f'sma{period}'] = data['close'].rolling(window=period).mean()
        
        return data


    @staticmethod
    def exponentialMovingAverage(data, period):
        """
        Exponential Moving Average (EMA)
        """
        print(f'Calculating EMA{period}...')
        data[f'ema{period}'] = data['close'].ewm(span=period, adjust=False).mean()
        return data  
    
    @staticmethod
    def rsi(df, periods=14, ema=True):
        """
        Calcula el índice de fuerza relativa (RSI) de una serie de precios de cierre.

        Args:
            df (DataFrame): Un DataFrame de pandas que debe contener una columna llamada 'close' con los precios de cierre.
            periods (int): El número de períodos que se utilizarán para calcular el RSI (por defecto: 14).
            ema (bool): Indica si se debe utilizar una media móvil exponencial (EMA) en lugar de una media móvil simple (SMA) para el cálculo (por defecto: True).

        Returns:
            pd.Series: Una serie de pandas que contiene los valores del RSI.
        """
        # Calcula el cambio en los precios de cierre
        close_delta = df['close'].diff()

        # Divide los cambios en dos series: uno para cambios positivos (up) y otro para cambios negativos (down)
        up = close_delta.clip(lower=0)
        down = -1 * close_delta.clip(upper=0)

        if ema:
            # Utiliza una media móvil exponencial (EMA)
            ma_up = up.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
            ma_down = down.ewm(com=periods - 1, adjust=True, min_periods=periods).mean()
        else:
            # Utiliza una media móvil simple (SMA)
            ma_up = up.rolling(window=periods).mean()
            ma_down = down.rolling(window=periods).mean()

        # Calcula el RSI
        rsi = ma_up / ma_down
        rsi = 100 - (100 / (1 + rsi))
        if ema:
            df[f'rsi{periods}ema'] = rsi
        else: df[f'rsi{periods}'] = rsi
        return df


