import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
from src.lib import MongoDbFunctions
from src.modules.ALGORITHM_MODULE import *
from src.system.secret import *

print("\033c")

# Conexión a la base de datos MongoDB
conn = MongoDbFunctions(
    DATABASE["host"],
    DATABASE["port"],
    DATABASE["username"],
    DATABASE["password"],
    DATABASE["dbname"],
    "CoreData",
)

# Obtener los datos y formatearlos
data = conn.findByField("interval", "1day")
symbol = data["symbol"]
data = CalculusFormater.formatData(data)
k = 40
# Calcular el RSI (puedes elegir si quieres RSI o RSI14EMA)
rsi = TechnicalIndicators.rsi(data,periods=k,ema=False)
rsi = TechnicalIndicators.simpleMovingAverage(data,period=14)
rsi = rsi[-300:]
print(rsi[-1:].head())
# Configurar el gráfico de velas
ohlc = rsi[['datetime', 'open', 'high', 'low', 'close', 'volume']]
ohlc['datetime'] = pd.to_datetime(ohlc['datetime'])
ohlc.set_index('datetime', inplace=True)

# Crear un subplot para el gráfico de velas
fig, axes = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
fig.suptitle(f'Gráfico de Velas de {symbol}')

# Configurar el gráfico de velas (utilizando 'ohlc' en lugar de 'type')
mpf.plot(ohlc, ax=axes[0], volume_panel=1, type='candle', style='binance')



# Configurar el gráfico de líneas para el RSI
axes[1].plot(rsi.index, rsi[f'rsi{k}'], label=f'RSI{k}', color='blue')


# Configurar etiquetas y leyenda para el gráfico de RSI
axes[1].set_xlabel('Fecha')
axes[1].set_ylabel('Valor RSI')
axes[1].set_title(f'Gráfico de dos líneas de RSI{k}')

# Añadir líneas horizontales en 80 y 20
axes[1].axhline(y=70, color='green', linestyle='--', label='Nivel 80')
axes[1].axhline(y=30, color='orange', linestyle='--', label='Nivel 20')

axes[1].legend()

# Añadir la línea SMA20 en el gráfico de velas (subgráfico 0


# Mostrar el gráfico
plt.tight_layout()
plt.show()
