
import pandas as pd
from tqdm import tqdm

BALANCE = 1000
BALANCE_INICIAL = 1000


def testing(df):
    # Establecer 'date' como índice
    df.set_index('date', inplace=True)

    # Filtrar para quedarse con la primera ocurrencia de cada día
    df = df.groupby(df.index.date).first()
    #print(df.columns)


    
    # Función para verificar si la fecha es lunes o viernes
    def is_monday_or_friday(date):
        if date.weekday() == 0:  # 0 representa el lunes
            return 'Lunes'
        elif date.weekday() == 4:  # 4 representa el viernes
            return 'Viernes'
        else:
            return None  # En caso de que no sea lunes ni viernes

    # Asegurarse de que el índice es de tipo datetime
    df.index = pd.to_datetime(df.index)

    # Aplicar la función al índice y crear una nueva columna 'day_type'
    df['day_type'] = df.index.map(is_monday_or_friday)

    #print(df["day_type"])

    num_mondays = len(df[df['day_type'] == 'Lunes'])
    num_fridays = len(df[df['day_type'] == 'Viernes'])

    # Mostrar el resultado
    #print(f'Días que son Lunes: {num_mondays}')
    #print(f'Días que son Viernes: {num_fridays}')

    
    df.dropna(subset=['day_type'], inplace=True)
    #print(df["day_type"])

    

    dates_to_remove = []

    # Iterar sobre el DataFrame
    for index, row in df.iterrows():
        if row['day_type'] == 'Lunes':
            # Buscar la siguiente fecha que sea viernes, considerando también si hay otro lunes en medio
            for next_index, next_row in df[(df.index > index)].iterrows():
                if next_row['day_type'] == 'Lunes':
                    # Si se encuentra otro lunes antes, eliminar solo este lunes
                    dates_to_remove.append(index)
                    break
                elif next_row['day_type'] == 'Viernes':
                    # Si se encuentra un viernes, verificar la diferencia de días
                    if (next_index - index).days != 4:
                        # Si la diferencia de días no es cuatro, eliminar ambos
                        dates_to_remove.extend([index, next_index])
                    break

    # Eliminar las fechas marcadas del DataFrame
    df = df.drop(dates_to_remove)

    # Mostrar el DataFrame resultante
    #print(df.columns)

    

    # Iterar sobre las filas de 'Viernes'
    for index, row in df[df['day_type'] == 'Lunes'].iterrows():
        # Calcular la fecha del lunes asociado
        friday_date = index + pd.Timedelta(days=4)

        # Verificar si el lunes existe en el DataFrame
        if friday_date in df.index:
            friday_row = df.loc[friday_date]

            growth = friday_row['close'] - row['close']


            # Asignar el valor de crecimiento a la fila de viernes
            df.at[index, 'growth'] = growth

    # Mostrar el DataFrame resultante
    #print(df.columns)
    #print(df[['day_type', 'growth']])

    
    # Filtrar el DataFrame original para obtener solo las filas con 'day_type' igual a 'Lunes'
    df_lunes = df[df['day_type'] == 'Lunes']


    new_df_lunes = df_lunes[['growth', 'minimunSignal']].copy()


    # Mostrar el nuevo DataFrame
    #print(new_df_lunes)


    
    # Sumar todas las veces que la columna 'minimunSignal' es True
    total_true = new_df_lunes['minimunSignal'].sum()

    # Mostrar el resultado
    #print("Número de veces que 'minimunSignal' es True:", total_true)


    
    # Filtrar para obtener solo las filas donde 'minimunSignal' es True
    df_true = new_df_lunes[new_df_lunes['minimunSignal'] == True]



    
    # Sumar todas las veces que la columna 'minimunSignal' es True
    total_growth = new_df_lunes['growth'].sum()

    # Mostrar el resultado
    return round(total_growth,2)
    




import os

# Ruta del directorio
directory = '/Users/oscarperezarruti/Documents/Documentos/Repositorios/trabajo-fin-grado-23/src/temp/testings'

# Lista para guardar los nombres de archivos
pkl_files = []

# Recorrer los archivos en el directorio
for filename in os.listdir(directory):
    if filename.endswith(".pkl"):
        # Añadir el archivo a la lista si termina en .pkl
        pkl_files.append(filename)

# Iterar a través de los activos y procesarlos con una barra de progreso
for pkl_file in tqdm(pkl_files, desc="Procesando"):

    df = pd.read_pickle(f"/Users/oscarperezarruti/Documents/Documentos/Repositorios/trabajo-fin-grado-23/src/temp/testings/{pkl_file}")
    resultados =  testing(df)

    BALANCE += resultados


crecimiento_porcentual = BALANCE*100/BALANCE_INICIAL

print(f"Balance: {round(BALANCE,2)} - ({round(crecimiento_porcentual,2)}%)")