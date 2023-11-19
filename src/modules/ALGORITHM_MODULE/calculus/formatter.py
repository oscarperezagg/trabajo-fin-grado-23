import pandas as pd






class CalculusFormater:
    
    @staticmethod
    def formatData(data):
        """
        Format data
        """
        data = pd.DataFrame(data["data"])
        # Convert 'datetime' to datetime type
        data['datetime'] = pd.to_datetime(data['datetime'])


        # Convert other columns to float type
        numeric_columns = list(data.columns)
        numeric_columns.remove("datetime")
        data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors='coerce')
        
        # Ordenar el DataFrame en orden ascendente por la columna 'datetime'
        return data.sort_values(by='datetime', ascending=True)
