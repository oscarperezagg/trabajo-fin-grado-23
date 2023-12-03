from src.lib.MONGODB import *
from src.system.secret import *
import pandas as pd
from src.modules.ALGORITHM_MODULE import *
import numpy as np

conn = MongoDbFunctions(
    DATABASE["host"],
    DATABASE["port"],
    DATABASE["username"],
    DATABASE["password"],
    DATABASE["dbname"],
    "CoreData",
)

assets = conn.findByMultipleFields(
    fields={"interval": "1day", "symbol": {"$ne": "SPX"}},
    custom=True,
    get_all=True,
    proyeccion={"_id": 0, "symbol": 1},
)
assets = [asset["symbol"] for asset in assets]

from progress.bar import Bar

import time

# Registra el tiempo de inicio
inicio = time.time()




# How to delta
def procesar(asset):

    resultado = parameters.beta(asset,lastDelta=False)

    


from tqdm import tqdm

# Supongamos que tienes una lista de activos llamada 'assets'


# Iterar a través de los activos y procesarlos con una barra de progreso
for asset in tqdm(assets[:100], desc="Procesando"):
    procesar(asset)





# Registra el tiempo de finalización
fin = time.time()

# Calcula el tiempo transcurrido
tiempo_transcurrido = fin - inicio

print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")
