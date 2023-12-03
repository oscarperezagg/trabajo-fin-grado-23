from src.modules.ALGORITHM_MODULE import *


validStocks = computation.computeData()
validStocks = computation.getValidStocks()
signals.signals(validStocks)
