from src.modules.ALGORITHM_MODULE import *
import pandas as pd
from src.modules.DATA_MODULE import *
from src.system.logging_config import logger


import os


class testing:
    @staticmethod
    def testingAndPerformance(mode, update=False, lastfiles=False):
        if update:
            logger.info("updating SPX")
            TDA_CoreData.updateAssets(mode, tradinghours=False)

            logger.info("updating stocks for 15min and 1day")
            AV_CoreData.updateAssets(
                mode, tradinghours=False, intervals=["1day", "15min"]
            )

            logger.info("Computing data and getting signals")

        if not lastfiles:
            logger.info("|   Computing data")
            valid_stocks = computation.computeData(testing=True)
            
            logger.info("|   Computing signals")
            # ELIMINAR
            valid_stocks = ["AMD"]
            signals.signals(valid_stocks,testing=True)

        
        # Ruta del directorio
        directory = signals.getTestingsPath()

        # Lista para guardar los nombres de archivos
        pkl_files = []
        BALANCE = 1000
        BALANCE_INICIAL = 1000
        # Recorrer los archivos en el directorio
        for filename in os.listdir(directory):
            if filename.endswith(".pkl"):
                # Añadir el archivo a la lista si termina en .pkl
                pkl_files.append(filename)

        # Iterar a través de los activos y procesarlos con una barra de progreso
        logger.info("Obteniendo resultados")
        for pkl_file in tqdm(pkl_files, desc="Procesando"):
            df = pd.read_pickle(f"{directory}/{pkl_file}")
            resultados = testing.stockTesting(df)

            BALANCE += resultados

        crecimiento_porcentual = BALANCE * 100 / BALANCE_INICIAL
        logger.info("Obteniendo resultados")
        logger.info(
            f"Balance: {round(BALANCE,2)} - ({round(crecimiento_porcentual,2)}%)"
        )

        updateStatus(False, mode)
        exit()

    def stockTesting(df):
        # Establecer 'date' como índice
        df.set_index("date", inplace=True)

        # Filtrar para quedarse con la primera ocurrencia de cada día
        df = df.groupby(df.index.date).first()
        # print(df.columns)
        true_count = df["minimunSignal"].sum()

        # Función para verificar si la fecha es lunes o viernes
        def is_monday_or_friday(date):
            if date.weekday() == 0:  # 0 representa el lunes
                return "Lunes"
            elif date.weekday() == 4:  # 4 representa el viernes
                return "Viernes"
            else:
                return None  # En caso de que no sea lunes ni viernes

        # Asegurarse de que el índice es de tipo datetime
        df.index = pd.to_datetime(df.index)

        # Aplicar la función al índice y crear una nueva columna 'day_type'
        df["day_type"] = df.index.map(is_monday_or_friday)

        df.dropna(subset=["day_type"], inplace=True)
        # print(df["day_type"])

        dates_to_remove = []

        # Iterar sobre el DataFrame
        for index, row in df.iterrows():
            if row["day_type"] == "Lunes":
                # Buscar la siguiente fecha que sea viernes, considerando también si hay otro lunes en medio
                for next_index, next_row in df[(df.index > index)].iterrows():
                    if next_row["day_type"] == "Lunes":
                        # Si se encuentra otro lunes antes, eliminar solo este lunes
                        dates_to_remove.append(index)
                        break
                    elif next_row["day_type"] == "Viernes":
                        # Si se encuentra un viernes, verificar la diferencia de días
                        if (next_index - index).days != 4:
                            # Si la diferencia de días no es cuatro, eliminar ambos
                            dates_to_remove.extend([index, next_index])
                        break

        # Eliminar las fechas marcadas del DataFrame
        df = df.drop(dates_to_remove)

        # Iterar sobre las filas de 'Viernes'
        for index, row in df[df["day_type"] == "Lunes"].iterrows():
            # Calcular la fecha del lunes asociado
            friday_date = index + pd.Timedelta(days=4)

            # Verificar si el lunes existe en el DataFrame
            if friday_date in df.index:
                friday_row = df.loc[friday_date]

                growth = friday_row["close"] - row["close"]

                # Asignar el valor de crecimiento a la fila de viernes
                df.at[index, "growth"] = growth

        # Filtrar el DataFrame original para obtener solo las filas con 'day_type' igual a 'Lunes'
        df_lunes = df[df["day_type"] == "Lunes"]
        new_df_lunes = df_lunes[["growth", "minimunSignal"]].copy()

        # Sumar todas las veces que la columna 'minimunSignal' es True
        total_growth = new_df_lunes.loc[new_df_lunes["minimunSignal"], "growth"].sum()

        # Mostrar el resultado
        return round(total_growth, 2)

    def getTestingsPath():
        # Obtén la ruta del directorio actual donde se encuentra el archivo.py
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construye la ruta al directorio "temp" que está fuera del directorio actual
        ruta_temp = os.path.abspath(
            os.path.join(directorio_actual, "..", "..", "temp", "testings")
        )

        # Convierte la ruta a una ruta absoluta (opcional)
        ruta_temp_absoluta = os.path.abspath(ruta_temp)

        return ruta_temp_absoluta


def getStatus():
    conn = None
    try:
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "status",
        )
        logger.debug("Obteniendo elemento de configuración")
        configDocu = conn.findByField("object", "error control")
        logger.debug("Configuración obtenida")
        conn.close()
        return (True, configDocu)
    except Exception as e:
        if conn:
            conn.close()
        logger.error("An error occurred: %s", str(e))
        return (False, e)


def updateStatus(status, action):
    config = getStatus()
    conn = None
    try:
        conn = MongoDbFunctions(
            DATABASE["host"],
            DATABASE["port"],
            DATABASE["username"],
            DATABASE["password"],
            DATABASE["dbname"],
            "status",
        )
        logger.debug("Modificando elemento de configuración")
        conn.updateByField(
            "object", "error control", {"status": status, "action": action}
        )
        logger.debug("Configuración modificada")
        conn.close()
        return (True, "")
    except Exception as e:
        if conn:
            conn.close()
        logger.error("An error occurred: %s", str(e))
        return (False, e)
