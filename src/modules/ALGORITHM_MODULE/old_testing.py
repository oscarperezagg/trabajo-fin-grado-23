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
        BALANCE_2018 = 0
        BALANCE_2019 = 0
        BALANCE_2020 = 0
        BALANCE_2021 = 0
        BALANCE_2022 = 0
        BALANCE_2023 = 0

        totals = {
            "2023": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
            "2022": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
            "2021": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
            "2020": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
            "2019": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
            "2018": {
                "negative_operations": 0,
                "positive_operations": 0,
            },
        }

        # Recorrer los archivos en el directorio
        for filename in os.listdir(directory):
            if filename.endswith(".pkl"):
                # Añadir el archivo a la lista si termina en .pkl
                pkl_files.append(filename)

        # Iterar a través de los activos y procesarlos con una barra de progreso
        logger.info("Obteniendo resultados")
        for pkl_file in tqdm(pkl_files, desc="|Procesando"):
            df = pd.read_pickle(f"{directory}/{pkl_file}")

            (
                year2023,
                year2022,
                year2021,
                year2020,
                year2019,
                year2018,
                operaciones,
            ) = testing.stockTesting(df)

            for year in ["2023", "2022", "2021", "2020", "2019", "2018"]:
                totals[year]["negative_operations"] += operaciones[year][
                    "negative_operations"
                ]
                totals[year]["positive_operations"] += operaciones[year][
                    "positive_operations"
                ]

            BALANCE_2023 += year2023
            BALANCE_2022 += year2022
            BALANCE_2021 += year2021
            BALANCE_2020 += year2020
            BALANCE_2019 += year2019
            BALANCE_2018 += year2018

        Balance_final = (
            BALANCE_2021
            + BALANCE_2022
            + BALANCE_2023
            + BALANCE_2020
            + BALANCE_2019
            + BALANCE_2018
        )
        logger.info("Obteniendo resultados")
        logger.info(f"Balance: {round(Balance_final,2)}")

        # Calculo de aciertos
        good_ops_2023 = (
            totals["2023"]["positive_operations"]
            * 100
            / (
                totals["2023"]["positive_operations"]
                + totals["2023"]["negative_operations"]
            )
        )
        good_ops_2022 = (
            totals["2022"]["positive_operations"]
            * 100
            / (
                totals["2022"]["positive_operations"]
                + totals["2022"]["negative_operations"]
            )
        )
        good_ops_2021 = (
            totals["2021"]["positive_operations"]
            * 100
            / (
                totals["2021"]["positive_operations"]
                + totals["2021"]["negative_operations"]
            )
        )

        good_ops_2020 = (
            totals["2020"]["positive_operations"]
            * 100
            / (
                totals["2020"]["positive_operations"]
                + totals["2020"]["negative_operations"]
            )
        )
        good_ops_2019 = (
            totals["2019"]["positive_operations"]
            * 100
            / (
                totals["2019"]["positive_operations"]
                + totals["2019"]["negative_operations"]
            )
        )

        good_ops_2018 = (
            totals["2018"]["positive_operations"]
            * 100
            / (
                totals["2018"]["positive_operations"]
                + totals["2018"]["negative_operations"]
            )
        )

        logger.info(
            f"--> 2021: {round(BALANCE_2021,2)} - {round(good_ops_2021,2)}% de acierto"
        )
        logger.info(
            f"--> 2022: {round(BALANCE_2022,2)} - {round(good_ops_2022,2)}% de acierto"
        )
        logger.info(
            f"--> 2023: {round(BALANCE_2023,2)} - {round(good_ops_2023,2)}% de acierto"
        )

        logger.info(
            f"--> 2020: {round(BALANCE_2020,2)} - {round(good_ops_2020,2)}% de acierto"
        )

        logger.info(
            f"--> 2019: {round(BALANCE_2019,2)} - {round(good_ops_2019,2)}% de acierto"
        )

        logger.info(
            f"--> 2018: {round(BALANCE_2018,2)} - {round(good_ops_2018,2)}% de acierto"
        )

        logger.setLevel("CRITICAL")
        updateStatus(False, mode)

        logger.setLevel("DEBUG")
        exit()

    def stockTesting(df):
        SIGNAL_FIELD = "minimunSignal"
        # Establecer 'date' como índice
        df.set_index("date", inplace=True)

        # Filtrar para quedarse con la primera ocurrencia de cada día
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
        new_df_lunes = df_lunes.copy()

        # Multiplicar todos los valores en 'growth' por 2
        new_df_lunes["growth"] = new_df_lunes["growth"] * 1

        RISK = 0.02
        ACCIONES = 1
        new_df_lunes["growth"] = ACCIONES * new_df_lunes["growth"]
        new_df_lunes["op_money"] = new_df_lunes["open"] * ACCIONES
        new_df_lunes["growth"] = new_df_lunes.apply(
            lambda row: -RISK * row["op_money"]
            if row["growth"] < -RISK * row["op_money"]
            else row["growth"],
            axis=1,
        )

        new_df_lunes["year"] = new_df_lunes.index.year

        # Filtrar y sumar para 2023
        total_growth_2023 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2023)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()

        # Filtrar y sumar para 2022
        total_growth_2022 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2022)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()

        # Filtrar y sumar para 2021
        total_growth_2021 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2021)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()
        total_growth_2020 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2020)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()
        total_growth_2019 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2019)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()
        total_growth_2018 = new_df_lunes.loc[
            (new_df_lunes["year"] == 2018)
            & (new_df_lunes[SIGNAL_FIELD])
            & (new_df_lunes["50smaSignal"])
            & (new_df_lunes["rsiSignal"]),
            "growth",
        ].sum()

        total_growth_2021 = round(total_growth_2021, 2)
        total_growth_2022 = round(total_growth_2022, 2)
        total_growth_2023 = round(total_growth_2023, 2)
        total_growth_2020 = round(total_growth_2020, 2)
        total_growth_2019 = round(total_growth_2019, 2)
        total_growth_2018 = round(total_growth_2018, 2)

        # Calcular los totales
        totals = {
            "2023": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2023)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2023)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
            "2022": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2022)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2022)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
            "2021": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2021)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2021)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
            "2020": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2020)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2020)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
            "2019": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2019)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2019)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
            "2018": {
                "negative_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2018)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] < 0),
                    "growth",
                ].shape[0],
                "positive_operations": new_df_lunes.loc[
                    (new_df_lunes["year"] == 2018)
                    & (new_df_lunes[SIGNAL_FIELD])
                    & (new_df_lunes["50smaSignal"])
                    & (new_df_lunes["rsiSignal"])
                    & (new_df_lunes["growth"] > 0),
                    "growth",
                ].shape[0],
            },
        }

        # Ahora, 'totals' es un diccionario que contiene los totales de crecimiento positivo y negativo para cada año.

        # Mostrar el resultado
        return (
            total_growth_2023,
            total_growth_2022,
            total_growth_2021,
            total_growth_2020,
            total_growth_2019,
            total_growth_2018,
            totals,
        )

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
