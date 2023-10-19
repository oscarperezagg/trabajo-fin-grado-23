import tkinter as tk

class AplicacionTkinter:
    def __init__(self, ventana):
        self.ventana = ventana
        self.ventana.title("Aplicación con Tkinter")

        self.label = tk.Label(ventana, text="")
        self.label.pack(pady=20)

        self.boton_interfaz1 = tk.Button(ventana, text="Interfaz 1", command=self.mostrar_interfaz1)
        self.boton_interfaz2 = tk.Button(ventana, text="Interfaz 2", command=self.mostrar_interfaz2)

        self.boton_interfaz1.pack()
        self.boton_interfaz2.pack()

    def mostrar_interfaz1(self):
        self.label.config(text="Interfaz 1 mostrada")

    def mostrar_interfaz2(self):
        self.label.config(text="Interfaz 2 mostrada")

if __name__ == "__main__":
    ventana = tk.Tk()
    app = AplicacionTkinter(ventana)
    ventana.mainloop()
