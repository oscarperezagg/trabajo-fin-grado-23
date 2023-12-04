
import pandas as pd

class testing:
    
    
    
    @staticmethod
    def testing():
        df = pd.read_pickle(f"/Users/oscarperezarruti/Documents/Documentos/Repositorios/trabajo-fin-grado-23/src/temp/AAPL.pkl")
        # Función para verificar si la fecha es lunes o viernes
        def is_monday_or_friday(date):
            if date.weekday() == 0:  # 0 representa el lunes
                return 'Lunes'
            elif date.weekday() == 4:  # 4 representa el viernes
                return 'Viernes'
            else:
                return None  # En caso de que no sea lunes ni viernes

        # Aplicar la función a la columna 'date' y crear una nueva columna 'day_type'
        df['day_type'] = df['date'].apply(is_monday_or_friday)
        
        print(df)