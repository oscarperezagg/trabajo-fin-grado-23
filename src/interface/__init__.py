import tkinter as tk
from tkinter import ttk

from matplotlib.backend_bases import NavigationToolbar2
from src.services import *
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import mplfinance as mpf
import pandas as pd

# Función para iniciar la aplicación tkinter
import matplotlib.pyplot as plt


ax = None


def iniciar_tkinter():
    def on_select(event):
        selected_option = combo.get()
        label.config(text=f"Seleccionaste: {selected_option}")
        # Eliminar el gráfico anterior (si existe)

        # Obtener los datos del símbolo seleccionado
        symbol = CRUDDatabase.getFullSymbolData(selected_option)
        # Crear un gráfico de barras (ejemplo)
        create_line_chart(symbol["data"])

    def update_combobox(event):
        search_term = search_entry.get().lower()
        filtered_options = [
            option for option in options if search_term in option.lower()
        ]
        combo["values"] = filtered_options

        # Configurar el valor del Combobox con la primera opción de filtered_options
        if filtered_options:
            combo.set(filtered_options[0])

    # Función para mostrar información adicional en una etiqueta
    def show_info(event):
        x, y = event.xdata, event.ydata
        # Agregar lógica para mostrar información adicional en una etiqueta o ventana emergente
        # Puedes obtener los datos asociados al punto (x, y) del gráfico y mostrarlos

    def create_line_chart(data):
        # Verificar si ya existe un gráfico en el lienzo y eliminarlo
        if hasattr(root, "canvas"):
            root.canvas.get_tk_widget().destroy()
        # Verificar si ya existe una barra de herramientas (toolbox) y eliminarla
        if hasattr(root, "toolbar"):
            root.toolbar.destroy()
        # Crear un DataFrame desde los datos
        data = data[::-1]
        df = pd.DataFrame(data)

        # Convertir las columnas OHLC y volumen a números
        df["open"] = pd.to_numeric(df["open"])
        df["high"] = pd.to_numeric(df["high"])
        df["low"] = pd.to_numeric(df["low"])
        df["close"] = pd.to_numeric(df["close"])
        df["volume"] = pd.to_numeric(df["volume"])

        # Convertir la columna 'datetime' en índice de tipo datetime
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        
        print(df.head())
        # Crear una figura más grande
        fig = Figure(figsize=(12, 6))
        # Limpiar el gráfico anterior (si existe)
        # Borrar el gráfico anterior

        ax = fig.add_subplot(111)  #
        mpf.plot(df, type="candle", ax=ax, style="charles")

        fig.canvas.mpl_connect("motion_notify_event", show_info)

        ax.set_xticks([])
        ax.set_yticks([])
        # Crear un lienzo de Matplotlib para tkinter
        canvas = FigureCanvasTkAgg(fig, master=root)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(pady=10)  # Usar pack

        # Agregar una barra de herramientas de navegación (toolbox)
        toolbar = NavigationToolbar2Tk(canvas, root)
        toolbar.update()
        toolbar.update_idletasks()

        toolbar.pack()  # Usar pack

        # Configurar el atributo 'canvas' en 'root' para que puedas acceder al lienzo
        root.canvas = canvas
        # Configurar el atributo 'toolbar' en 'root' para que puedas acceder a la barra de herramientas
        root.toolbar = toolbar

    # Crear una ventana principal
    root = tk.Tk()
    root.title("Aplicación con Dropdown")

    # Establecer el tamaño mínimo de la ventana
    root.minsize(1000, 800)

    # Crear una etiqueta
    label = tk.Label(root, text="Selecciona una opción:")
    label.pack(pady=10)

    # Crear una etiqueta para el campo de búsqueda
    search_label = tk.Label(root, text="Buscar opción:")
    search_label.pack(pady=5)

    # Crear una entrada para búsqueda
    search_entry = tk.Entry(root)
    search_entry.pack(pady=5)
    search_entry.bind("<KeyRelease>", update_combobox)

    # Crear un menú desplegable con opciones
    options = CRUDDatabase.getSymbols()
    combo = ttk.Combobox(root, values=options)
    combo.pack(pady=5)
    combo.bind("<<ComboboxSelected>>", on_select)

    # Configurar el valor inicial del Combobox con la primera opción
    if options:
        combo.set(options[0])

    # Iniciar la aplicación
    root.mainloop()
