import tkinter as tk
from tkinter import ttk

def on_select(event):
    selected_option = combo.get()
    label.config(text=f"Seleccionaste: {selected_option}")

# Crear una ventana principal
root = tk.Tk()
root.title("Aplicación con Dropdown")

# Crear una etiqueta
label = tk.Label(root, text="Selecciona una opción:")
label.pack(pady=10)

# Crear un menú desplegable con opciones
options = ["Opción 1", "Opción 2", "Opción 3"]
combo = ttk.Combobox(root, values=options)
combo.pack(pady=5)
combo.bind("<<ComboboxSelected>>", on_select)

# Iniciar la aplicación
root.mainloop()
