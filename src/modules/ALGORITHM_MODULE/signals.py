from datetime import datetime

import os
import threading
import pandas as pd
from tqdm import tqdm
import webbrowser
import os

FAANG = ["FB", "AAPL", "AMZN", "NFLX", "GOOG"]


class signals:
    ############################################################
    #######################  SIGNALS  ##########################
    ############################################################

    @staticmethod
    def signals(validStocks, testing=False):
        path = signals.getTempPath()
        signalsPath = signals.getTestingsPath()
        print("\n")
        buy_signals = {}
        for stock in tqdm(validStocks, desc="Procesando stocks"):
            try:
                df = pd.read_pickle(f"{path}/{stock}.pkl")
                df["betaSignal"] = df.apply(
                    lambda row: signals.betaSignal(row, stock), axis=1
                )

                df["200smaSignal"] = df.apply(
                    lambda row: signals.movingAverageSignal200(row), axis=1
                )
                df["50smaSignal"] = df.apply(
                    lambda row: signals.movingAverageSignal50(row), axis=1
                )

                df["reportSignal"] = df.apply(
                    lambda row: signals.ReportSignal(row), axis=1
                )

                df["minimunSignal"] = (
                    df["betaSignal"] & df["200smaSignal"] & df["reportSignal"]
                )

                df["rsiSignal"] = df.apply(lambda row: signals.rsiSignal(row), axis=1)

                df["badRsiSignal"] = df.apply(
                    lambda row: signals.badRsiSignal(row), axis=1
                )

                # Desplazando los valores de cada columna especificada una posición adelante
                # No trabajamos con los datos del mismo día pues técnicamente no los tenemos, tenemos los del día anterior
                df["betaSignal"] = df["betaSignal"].shift(1)
                df["200smaSignal"] = df["200smaSignal"].shift(1)
                df["50smaSignal"] = df["50smaSignal"].shift(1)
                df["reportSignal"] = df["reportSignal"].shift(1)
                df["minimunSignal"] = df["minimunSignal"].shift(1)
                df["rsiSignal"] = df["rsiSignal"].shift(1)
                df["badRsiSignal"] = df["badRsiSignal"].shift(1)

                if df.iloc[-1]["minimunSignal"]:
                    buy_signals[stock] = [
                        "- [BASIC] Beta > 1.4, Above 200 SMA, Movement after report positive"
                    ]

                    if df.iloc[-1]["50smaSignal"]:
                        buy_signals[stock].append("- [GOOD] Above 50 SMA ")
                    else:
                        buy_signals[stock].append("- [BAD] Below 50 SMA ")

                    if df.iloc[-1]["rsiSignal"]:
                        buy_signals[stock].append("- [GOOD] RSI under 30")
                    elif df.iloc[-1]["badRsiSignal"]:
                        buy_signals[stock].append("- [BAD] RSI above 70")
                    else:
                        buy_signals[stock].append("- [NEUTRAL] RSI between 30 and 70")

                df.to_pickle(f"{signalsPath}/{stock}.pkl")

            except Exception as e:
                print(f"Error procesando {e}")
                pass
        print("\n")

        if not testing:
            signals.createResultsTable(buy_signals)

    def betaSignal(row, stock):
        return row["beta"] > 1.4 or stock in FAANG

    def movingAverageSignal200(row):
        return row["close"] > row["SMA_200"]

    def movingAverageSignal50(row):
        return row["close"] > row["SMA_50"]

    def ReportSignal(row):
        return row["reportPriceMovement"] > 0

    def rsiSignal(row):
        return row["RSI_14"] < 30

    ######### BAD SIGNALS #########

    def badRsiSignal(row):
        return row["RSI_14"] > 70

    #############################################################
    #######################  SUPPORT  ##########################
    #############################################################

    def getTempPath():
        # Obtén la ruta del directorio actual donde se encuentra el archivo.py
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construye la ruta al directorio "temp" que está fuera del directorio actual
        ruta_temp = os.path.abspath(os.path.join(directorio_actual, "..", "..", "temp"))

        # Convierte la ruta a una ruta absoluta (opcional)
        ruta_temp_absoluta = os.path.abspath(ruta_temp)

        return ruta_temp_absoluta

    def getTempResults():
        # Obtén la ruta del directorio actual donde se encuentra el archivo.py
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construye la ruta al directorio "temp" que está fuera del directorio actual
        ruta_temp = os.path.abspath(
            os.path.join(directorio_actual, "..", "..", "temp", "results")
        )

        # Convierte la ruta a una ruta absoluta (opcional)
        ruta_temp_absoluta = os.path.abspath(ruta_temp)

        return ruta_temp_absoluta

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

    def createResultsTable(acciones):
        html = """
                <html>
                <head>
                    <style>
                        body {
                            display: flex;
                            justify-content: center;
                            align-items: center;
                        
                            margin: 0;
                            margin-top: 5%;
                            margin-bottom: 5%;
                        }
                        table {
                            border-collapse: collapse;
                            width: 50%; /* O el ancho que prefieras */
                        }
                        th, td {
                            border: 1px solid black;
                            padding: 8px;
                            text-align: left;
                        }
                        th {
                            background-color: #f2f2f2;
                        }
                    </style>
                </head>
                <body>
                    <table>
                        <tr>
                            <th>Acción</th>
                            <th>Detalles</th>
                        </tr>
                """

        for accion, detalles in acciones.items():
            html += f"  <tr>\n    <td>{accion}</td>\n    <td>{'<br>'.join(detalles)}</td>\n  </tr>\n"

        html += """
        </table>
        </body>
        </html>
        """

        # Suponiendo que 'tabla_html' es tu código HTML generado

        # Guardar el HTML en un archivo
        # Obtener la fecha y hora actual
        now = datetime.now()

        # Formatear la fecha y hora en el formato deseado (año-mes-día_hora-minuto-segundo)
        formatted_date = now.strftime("%Y-%m-%d_%H-%M-%S")

        resultsPath = signals.getTempResults()
        nombre_archivo = f"{resultsPath}/tabla_acciones_{formatted_date}.html"
        with open(nombre_archivo, "w") as archivo:
            archivo.write(html)

        # Crear y empezar un nuevo hilo para abrir el navegador
        hilo_navegador = threading.Thread(
            target=signals.abrir_navegador, args=(nombre_archivo,)
        )
        hilo_navegador.start()

    def abrir_navegador(nombre_archivo):
        webbrowser.open("file://" + os.path.realpath(nombre_archivo))
