import duckdb
import pandas as pd

raw_path = 'data/BTCUSDT/tradebook/raw_data.db'
analytics_path = 'data/BTCUSDT/tradebook/analytics.db'

try:
    con = duckdb.connect(raw_path)
    con.execute(f"ATTACH '{analytics_path}' AS analytics")

    print(f"{'Tabla':<25} | {'Registros':<12} | {'Primer Timestamp'} | {'Último Timestamp'}")
    print("-" * 80)

    # Check raw_data
    """ raw_info = con.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM agg_trades").fetchone()
    first_raw = pd.Timestamp(raw_info[1], unit='us') if raw_info[1] else "N/A"
    last_raw = pd.Timestamp(raw_info[2], unit='us') if raw_info[2] else "N/A"
    print(f"{'agg_trades (RAW)':<25} | {raw_info[0]:<12,} | {first_raw} | {last_raw}") """

    # Check analytics tables
    tables = [
        ('analytics.ohlc_1_minutes', 'open_time'),
        ('analytics.volume_profile', 'open_time'),
        ('analytics.market_context_4_hours', 'open_time')
    ]

    for table, time_col in tables:
        try:
            info = con.execute(f"SELECT COUNT(*), MIN({time_col}), MAX({time_col}) FROM {table}").fetchone()
            count = info[0]
            first_time = pd.Timestamp(info[1], unit='us') if info[1] else "N/A"
            last_time = pd.Timestamp(info[2], unit='us') if info[2] else "N/A"
            print(f"{table:<25} | {count:<12,} | {first_time} | {last_time}")
        except Exception as e:
            print(f"{table:<25} | {'ERROR':<12} | {str(e)[:30]}...")
            
    #con.execute("drop table analytics.ohlc_1_minutes").fetchone()
    #con.execute("drop table analytics.volume_profile").fetchone()
    #con.execute("drop table analytics.market_context_4_hours").fetchone() # Limpieza de tabla de contexto para forzar recalculo
    con.close()
except Exception as e:
    print(f"Error al conectar: {e}")
