import os
import pandas as pd
from tqdm import tqdm

FAANG = ["FB", "AAPL", "AMZN", "NFLX", "GOOG"]


class signals:
    ############################################################
    #######################  SIGNALS  ##########################
    ############################################################

    @staticmethod
    def signals(validStocks):
        path = signals.getTempPath()
        print(len(validStocks))

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
                df["superMinimunSignal"] = df["minimunSignal"] & df["50smaSignal"]

                if df.iloc[-1]["minimunSignal"]:
                    buy_signals[stock] = [
                        "- Requerimiento básico cumplidos (Beta, 200 SMA, Presentación de resultados"
                    ]
                    if df.iloc[-1]["superMinimunSignal"]:
                        buy_signals[stock].append(
                            "- Por encima de la media de 200 y 50 días"
                        )

            except Exception as e:
                print(e)

        signals.createResultsTable(buy_signals)

    def betaSignal(row, stock):
        return row["beta"] > 1.4 or stock in FAANG

    def movingAverageSignal200(row):
        return row["close"] > row["SMA_200"]

    def movingAverageSignal50(row):
        return row["close"] > row["SMA_50"]

    def ReportSignal(row):
        return row["reportPriceMovement"] > 0

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
        import webbrowser
        import os

        # Suponiendo que 'tabla_html' es tu código HTML generado

        # Guardar el HTML en un archivo
        nombre_archivo = r"/Users/oscarperezarruti/Documents/Documentos/Repositorios/trabajo-fin-grado-23/src/temp/results/tabla_acciones.html"
        with open(nombre_archivo, "w") as archivo:
            archivo.write(html)

        # Ruta absoluta al archivo
        ruta_absoluta = os.path.abspath(nombre_archivo)

        # Abrir en el navegador
        webbrowser.open("file://" + os.path.realpath(nombre_archivo))
