'''
    Este moodulo contiene funciones para el trabajo con datos de criptomonedas obtenidos de los 
    los principales exchenges en el mercado. 

'''

from binance.client import Client
import pandas as pd
import os
from datetime import datetime, timedelta
import time




def binance_adquirir_data_15m(fecha_inicio='', fecha_fin='', symbol = 'BTCUSDT'):
    '''
        Esta función adquiere datos de precios de criptomonedas en la temporalidad de 15 minutos
        usando como proveedor al exchange Binance. 
            - El simbolo por defecto para descargar los datos es Bitcoin. 
            - Fecha_inicio y fecha_fin se deben pasar con el formato AAAA-MM-DD 
              si no se especifica alguna de las fechas se retorna una cadena de texto de observación. 
            - Retorna un dataframe de pandas con los datos de mercado de un activo 
        
    '''
    client = Client()   # Crear cliente Binance (solo lectura, sin claves)

    start_str = fecha_inicio
    end_str = fecha_fin
    mensaje_observacion = "OBSERVACIÓN: " + "no se han ingresado fechas para la adquisición de datos!".title()

    if start_str and end_str: #Comprueba si se ingresó las fechas de adquisición de datos 

        intervalo = Client.KLINE_INTERVAL_15MINUTE
        start_time = pd.to_datetime(start_str)
        end_time = pd.to_datetime(end_str)
        delta = timedelta(minutes=15 * 1000)  # Binance permite 1000 velas por llamada (~10 días)
        all_data = []

        while start_time < end_time:
            next_time = min(start_time + delta, end_time)
            print(f"Descargando desde {start_time} hasta {next_time}...")
            try:
                klines = client.get_historical_klines(
                    symbol,
                    interval=intervalo,
                    start_str=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_str=next_time.strftime("%Y-%m-%d %H:%M:%S")
                )
                if not klines:
                    break
                all_data.extend(klines)
            except Exception as e:
                print(f"Error: {e}")
                break

            start_time = next_time
            time.sleep(0.5)  # prevenir límite de peticiones

        # Convertir a DataFrame
        df = pd.DataFrame(all_data, columns=[
            "timestamp", "open", "high", "low", "close", "volume", "close_time",
            "quote_asset_volume", "num_trades", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ])
    
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
        df.set_index("timestamp", inplace=True)
        df = df.astype(float, errors="ignore")

        return df

      
        
    else:
        return mensaje_observacion
    


data = binance_adquirir_data_15m('2025-07-10','2025-07-12')
print(data)
