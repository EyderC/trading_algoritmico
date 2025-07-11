from openbb import obb
from binance.client import Client
import pandas as pd
import os
from datetime import datetime, timedelta
import time

'''
obb.user.preferences.output_type = "dataframe"  # configura el formato de salida de openbb como un dataframe de pandas
data_yfinance = obb.equity.price.historical("BTC-USD", provider="yfinance") # especificamos al metodo historical datos de bitcoin de yahoo finance, por defecto historical proporciona datos diarios pero podemos configurar otras temporalidades
print(data_yfinance.tail())

# Si tienes claves API de Binance, inclúyelas. Para solo lectura, puedes dejar strings vacíos:
client = Client(api_key="", api_secret="")

# Obtener datos históricos de velas (candlesticks)
klines = client.get_historical_klines("BTCUSDT", interval=Client.KLINE_INTERVAL_1DAY, start_str="2025-01-01", end_str="2025-07-10")

# Convertir a DataFrame
data_binance = pd.DataFrame(klines, columns=[
    "timestamp", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "num_trades", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
])

# Convertir tiempo a fecha legible
data_binance["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')

print(data_binance.tail())
'''
# Crear cliente Binance (solo lectura, sin claves)
client = Client()

def obtener_binance_15min(symbol="BTCUSDT", start_str="2025-01-01", end_str="2025-07-10"):
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

# Ejecutar descarga
df_binance = obtener_binance_15min()

# Mostrar muestra
print(df_binance.tail())

# Guardar a CSV (opcional)
df_binance.to_csv("btc_binance_15m_2025.csv")


##############################################################
'''
    TAREAS 
    
    1 -Escribir funcione de adquisición de datos def coinbase_adquirir_data_15m(activo = 'BTCUSDT')
    2- crear un subdirectorio en /tradin_algoritmico para almacenar los datos descargados en formato csv. 
    3- crear un script para descargar los datos de 15min todos los dias y se almacenen en el subdirectorio en formato csv. 
 


'''