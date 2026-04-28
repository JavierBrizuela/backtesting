from download_agg_trades_binance import AggTradesBinanceDownloader
from agg_trades_to_db import AggTradeDB
import pandas as pd
import os

simbol = 'BTCUSDT'
interval = '1 minutes'
resolution = 10
raw_path = f'data/{simbol}/tradebook/raw_data.db'
analytics_path = f'data/{simbol}/tradebook/analytics.db'
table = 'agg_trades'
context_interval='4 hours'  # Market Structure se calcula en 4H para contexto de tendencia

def agg_trades_monthly(start_year, start_month, end_year, end_month, end_day, simbol):
    current_year = start_year
    current_month = start_month
    while (current_year < end_year) or (current_year == end_year and current_month < end_month):
        print(f"\n[MES] Procesando {current_year}-{current_month:02d}...")
        csv_path = agg_trades_df.download_agg_trades_montly(simbol, current_year, current_month)
        if csv_path:
            agg_trades_DB.import_csv_to_db(csv_path, table)
            os.remove(csv_path) # Limpiar el CSV temporal
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    # Procesar los días del mes actual si es necesario
    agg_trades_daily(current_year, current_month, 1, end_day, simbol)
   
def agg_trades_daily(year, month, start_day, end_day, simbol): 
    for day in range(start_day, end_day):
        print(f"\n[DIA] Procesando {year}-{month:02d}-{day:02d}...")
        csv_path = agg_trades_df.download_agg_trades_daily(simbol, year, month, day)
        if csv_path:
            agg_trades_DB.import_csv_to_db(csv_path, table)
            os.remove(csv_path) # Limpiar el CSV temporal

# --- FLUJO PRINCIPAL ---

today = pd.Timestamp.now()
end_year = today.year
end_month = today.month
end_day = today.day

os.makedirs(os.path.dirname(raw_path), exist_ok=True)

agg_trades_df = AggTradesBinanceDownloader()
agg_trades_DB = AggTradeDB(raw_path, analytics_path,UTC=True)

# Obtener tablas existentes
tables = agg_trades_DB.con.execute("SHOW TABLES;").fetchdf()

if table in tables['name'].values:
    # Obtener el último timestamp guardado
    res = agg_trades_DB.con.execute(f"SELECT MAX(timestamp) FROM {table}").fetchone()[0]
    if res:
        last_date = pd.Timestamp(res, unit='us') # agg_trades usa microsegundos
        print(f"\n[SQL]Último timestamp en la base de datos: {last_date}")
        
        start_year = last_date.year
        start_month = last_date.month
        start_day = last_date.day + 1 # Comenzar desde el día siguiente
        
        # 1. Actualizar meses completos si faltan
        if (start_year < end_year) or (start_year == end_year and start_month < end_month):
            print(f"\n[MES]Actualizando datos mensuales desde {start_year}-{start_month}...")
            agg_trades_monthly(start_year, start_month, end_year, end_month, end_day, simbol)

        # 2. Actualizar días sueltos si estamos en el mes actual
        if (start_year == end_year and start_month == end_month and start_day < end_day):
            print(f"\n[DIA]Actualizando datos diarios desde {start_year}-{start_month}-{start_day}...")
            agg_trades_daily(end_year, end_month, start_day, end_day, simbol)
    else:
        # Si la tabla está vacía, empezar desde 2025
        print("\n[SQL]Tabla existente pero vacía. Iniciando descarga desde 2025-01.")
        agg_trades_monthly(2025, 1, end_year, end_month, end_day, simbol)
else:  
    print(f"\n[SQL]Base de datos no encontrada. Iniciando descarga completa hasta: {end_year}-{end_month}-{end_day-1}")
    agg_trades_monthly(2025, 1, end_year, end_month, end_day, simbol)

# --- POST-PROCESAMIENTO ---
print("\n[ANÁLISIS] Sincronizando tablas de OHLC, Volume Profile y Market Context...")
agg_trades_DB.create_ohlc_table_from_aggtrades(interval)
agg_trades_DB.create_volume_profile_table(resolution, interval)
agg_trades_DB.create_market_context_table(context_interval)

print("\n[FIN] Proceso completado exitosamente.")
agg_trades_DB.close_connection()
