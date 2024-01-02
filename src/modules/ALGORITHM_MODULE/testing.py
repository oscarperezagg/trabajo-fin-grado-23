from src.modules.ALGORITHM_MODULE import *
import pandas as pd
from src.modules.DATA_MODULE import *
from src.system.logging_config import logger

N = 100

import os


class testing:
    @staticmethod
    def testingAndPerformance(mode, update=False, lastfiles=True):
        if update:
            logger.info("updating SPX")

            updateStatus(True, "UpdateSPX")
            finished = False
            iteration = 0
            while not finished and iteration < N:
                # Ejecutamos la función en un hilo aparte
                TDA_CoreData.updateAssets("UpdateSPX", tradinghours=False)
                # Comprobamos el estado de la tarea en la base de datos
                finished = not getErrorStatus()
                if not finished:
                    logger.error("Error controlado durante la tarea %s", mode)
                iteration += 1

            logger.info("updating stocks for 1day")

            updateStatus(True, "UpdateStocks")
            finished = False
            iteration = 0
            while not finished and iteration < N:
                # Ejecutamos la función en un hilo aparte
                AV_CoreData.updateAssets(
                    "UpdateStocks", tradinghours=False, intervals=["1day"]
                )
                # Comprobamos el estado de la tarea en la base de datos
                finished = not getErrorStatus()
                if not finished:
                    logger.error("Error controlado durante la tarea %s", mode)
                iteration += 1
            logger.info("Computing data and getting signals")

        if not lastfiles:
            logger.info("|   Computing data")
            valid_stocks = computation.computeData(testing=True)

            logger.info("|   Computing signals")

            signals.signals(valid_stocks, testing=True)

        # Ruta del directorio
        directory = signals.getTestingsPath()
        # Lista para guardar los nombres de archivos
        pkl_files = []
        # Recorrer los archivos en el directorio
        for filename in os.listdir(directory):
            if filename.endswith(".pkl"):
                # Añadir el archivo a la lista si termina en .pkl
                pkl_files.append(filename)

        standard_and_poors = computation.standard_and_poors()

        aciertos_total = 0
        evaluados_total = 0
        error_medio_total = 0.0
        logger.info("Obteniendo resultados")
        for pkl_file in tqdm(pkl_files, desc="|Procesando"):
            df = pd.read_pickle(f"{directory}/{pkl_file}")
            calculated_stock = testing.stockTesting(df, standard_and_poors)
            if calculated_stock is False:
                continue
            # Calculamos el porcentaje de aciertos
            aciertos = calculated_stock[calculated_stock["mismo_signo"] == True].shape[
                0
            ]
            total = calculated_stock.shape[0]
            # Calculamos error medio para los aciertos
            if aciertos != 0:
                error_medio = (
                    calculated_stock[calculated_stock["mismo_signo"] == True][
                        "abs_error"
                    ].sum()
                    / aciertos
                )

                aciertos_total += aciertos
                error_medio_total = (error_medio_total + error_medio) / 2

            evaluados_total += total

        logger.info("Resultados obtenidos")
        logger.info("Aciertos: %s", aciertos_total)
        logger.info("Evaluados: %s", evaluados_total)
        logger.info("Error medio: %s", error_medio_total)
        updateStatus(False, mode)

        logger.setLevel("DEBUG")
        exit()

    def stockTesting(stock, spx):
        stock = testing.valid_monday_friday(stock)

        if stock.shape[0] == 0:
            return False

        filtered_spx = testing.format_spx(spx, stock)

        if filtered_spx.shape[0] != stock.shape[0]:
            return False

        # Calculamos el crecimiento para todas las fechas lunes
        calculated_stock = stock.copy()

        # Create a new column 'crecimiento' initialized with None
        calculated_stock["crecimiento"] = None

        # Loop over the DataFrame
        for index, row in calculated_stock.iterrows():
            if row["es_lunes"]:
                # Get the 'ultimo_dia_semana' date for the current row
                last_day_date = row["ultimo_dia_semana"]

                # Find the corresponding Friday's row
                friday_row = calculated_stock[calculated_stock["date"] == last_day_date]

                if not friday_row.empty:
                    # Calculate the percentage growth
                    open_value = row["open"]
                    close_value = friday_row.iloc[0]["close"]
                    growth = ((close_value - open_value) / open_value) * 100

                    # Assign the calculated growth to the 'crecimiento' column
                    calculated_stock.at[index, "crecimiento"] = growth
                else:
                    logger.critical("Falta un viernes en el dataframe")

        # Create a new column 'crecimiento' in filtered_spx
        filtered_spx["crecimiento"] = None

        # Loop over the DataFrame
        for index, row in filtered_spx.iterrows():
            if row["es_lunes"]:
                # Get the 'ultimo_dia_semana' date for the current row
                last_day_date = row["ultimo_dia_semana"]

                # Find the corresponding Friday's row
                friday_row = filtered_spx[filtered_spx["date"] == last_day_date]

                if not friday_row.empty:
                    # Calculate the percentage growth
                    open_value = row["open"]
                    close_value = friday_row.iloc[0]["close"]
                    growth = ((close_value - open_value) / open_value) * 100
                    if growth == 0:
                        logger.critical("Growth is 0")
                    # Assign the calculated growth to the 'crecimiento' column
                    filtered_spx.at[index, "crecimiento"] = growth
                else:
                    logger.critical("Falta un viernes en el dataframe")

        calculated_stock = calculated_stock.copy()
        calculated_stock["crecimiento_spx"] = 0.0
        calculated_stock["mismo_signo"] = False
        calculated_stock["abs_error"] = 0.0
        calculated_stock["abs_pertecentaje_error"] = 0.0

        # Loop over the DataFrame
        for index, row in calculated_stock.iterrows():
            if row["es_lunes"]:
                # Get the 'ultimo_dia_semana' date for the current row
                stock_monday = row["date"]

                # Find the corresponding Friday's row
                spx_monday = filtered_spx[filtered_spx["date"] == stock_monday]

                if not spx_monday.empty:
                    crecimiento_stock = row["crecimiento"]
                    crecimiento_spx = spx_monday.iloc[0]["crecimiento"]
                    calculated_stock.at[index, "crecimiento_spx"] = float(
                        crecimiento_spx
                    )
                    # Si son del mismo signo
                    if (crecimiento_stock > 0 and crecimiento_spx > 0) or (
                        crecimiento_stock < 0 and crecimiento_spx < 0
                    ):
                        calculated_stock.at[index, "mismo_signo"] = True
                        # Calculamos diferencia entre crecimiento stock y spx
                        calculated_stock.at[index, "abs_error"] = float(
                            abs(crecimiento_stock - crecimiento_spx)
                        )
                        calculated_stock.at[index, "abs_pertecentaje_error"] = float(
                            abs(crecimiento_stock - crecimiento_spx)
                            / abs(crecimiento_spx)
                            * 100
                        )

                else:
                    logger.critical("Falta un lunes en el dataframe")

        return calculated_stock

    def format_spx(spx, stock):
        # Filtramos spx por las fechas de la columna date de stock
        # Step 1: Extract unique dates from filtered_stock
        unique_dates = stock["date"].unique()

        # Step 2: Filter spx DataFrame using these dates
        filtered_spx = spx[spx["date"].isin(unique_dates)]
        filtered_spx = filtered_spx.copy()
        filtered_spx.loc[:, "es_lunes"] = filtered_spx["date"].dt.day_name() == "Monday"
        filtered_spx.loc[:, "es_viernes"] = (
            filtered_spx["date"].dt.day_name() == "Friday"
        )

        filtered_spx = filtered_spx.copy()
        # Calcula el primer día (lunes) y último día (viernes) de cada semana
        filtered_spx["primer_dia_semana"] = filtered_spx["date"] - pd.to_timedelta(
            filtered_spx["date"].dt.dayofweek, unit="D"
        )
        filtered_spx["ultimo_dia_semana"] = filtered_spx[
            "primer_dia_semana"
        ] + pd.DateOffset(days=4)
        return filtered_spx

    def valid_monday_friday(stock):
        stock = stock.copy()
        # Calcula el primer día (lunes) y último día (viernes) de cada semana
        stock["primer_dia_semana"] = stock["date"] - pd.to_timedelta(
            stock["date"].dt.dayofweek, unit="D"
        )
        stock["ultimo_dia_semana"] = stock["primer_dia_semana"] + pd.DateOffset(days=4)

        filtered_stock = stock[
            (stock["date"] == stock["primer_dia_semana"])
            | (stock["date"] == stock["ultimo_dia_semana"])
        ]
        filtered_stock = filtered_stock.copy()

        ## Define the function 'existe_otra_fila'
        def existe_otra_fila(fecha, columna):
            return filtered_stock[(filtered_stock[columna] == fecha)].shape[0] > 0

        # Apply the logic for 'es_lunes' and 'es_viernes'
        filtered_stock["es_lunes"] = filtered_stock["date"].dt.day_name() == "Monday"
        filtered_stock["es_viernes"] = filtered_stock["date"].dt.day_name() == "Friday"

        # Add conditions for 'eliminar_lunes' and 'eliminar_viernes'
        filtered_stock["eliminar_lunes"] = filtered_stock[
            "es_lunes"
        ] & filtered_stock.apply(
            lambda row: not existe_otra_fila(row["ultimo_dia_semana"], "date"), axis=1
        )
        filtered_stock["eliminar_viernes"] = filtered_stock[
            "es_viernes"
        ] & filtered_stock.apply(
            lambda row: not existe_otra_fila(row["primer_dia_semana"], "date"), axis=1
        )

        # Drop the rows marked for elimination
        filtered_stock = filtered_stock[
            ~(filtered_stock["eliminar_lunes"] | filtered_stock["eliminar_viernes"])
        ]

        # Drop the columns used for marking rows
        filtered_stock.drop(
            columns=["eliminar_lunes", "eliminar_viernes"], inplace=True
        )
        
        
        filtered_stock["finalSignal"] = (
            filtered_stock["minimunSignal"]
        )
        # Step 1: Filter rows where 'minimumSignal' is False and 'es_lunes' is True
        rows_to_remove = filtered_stock[
            (filtered_stock["finalSignal"] == False)
            & (filtered_stock["es_lunes"] == True)
        ]

        # Step 2: Extract dates from 'ultimo_dia_semana'
        dates_to_remove = rows_to_remove["ultimo_dia_semana"]

        # Step 3: Remove rows based on condition
        # Step 3: Remove rows based on condition
        filtered_stock = filtered_stock[
            ~(
                (filtered_stock["finalSignal"] == False)
                & (filtered_stock["es_lunes"] == True)
            )
        ]

        # Step 4: Remove additional rows based on dates in dates_to_remove
        filtered_stock = filtered_stock[~filtered_stock["date"].isin(dates_to_remove)]

        return filtered_stock

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


def getErrorStatus():
    status = getStatus()
    if status[0]:
        return status[1]["status"]
    else:
        logger.critical("An error ocurred while getting error control document")


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
