import duckdb
import pandas as pd

class AggTradeDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self.con = duckdb.connect(database=self.db_path)
    
    def save_df_to_db(self, df, table):
        if df.empty:
            print("⚠️ No hay datos en el dataframe para crear la tabla.")
            return None
        self.con.execute(f'''
            CREATE TABLE IF NOT EXISTS {table} (
                agg_trade_id BIGINT PRIMARY KEY,
                price DOUBLE,
                quantity DOUBLE,
                first_trade_id BIGINT,
                last_trade_id BIGINT,
                timestamp BIGINT,
                is_buyer_maker BOOLEAN,
                is_best_match BOOLEAN
            )
        ''')
        count_before = self.con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
        self.con.execute(f'''
                         INSERT OR IGNORE INTO {table} SELECT * FROM df
        ''')
        count_after = self.con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
        print(f"Registros antes: {count_before}, después: {count_after}, añadidos: {count_after - count_before}")
        
    def ohlc_table(self, interval, start, end):
        interval_name = interval.replace(" ", "_")
        query = f"""
        CREATE TABLE IF NOT EXISTS ohlc_{interval_name} AS
        SELECT
            time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000) ) AS open_time,
            FIRST(price ORDER BY timestamp ASC) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            FIRST(price ORDER BY timestamp DESC) AS close,
            SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END) AS sell_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
            SUM(quantity) AS volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE -quantity END) AS delta,
            MAX(last_trade_id) - MIN(first_trade_id) + 1 AS trade_count,
            CASE 
                WHEN close >= open THEN 'green'
                ELSE 'red'
            END AS color_smart_money
        FROM agg_trades
        WHERE to_timestamp(timestamp / 1000000) >= '{start}'::TIMESTAMP
        AND to_timestamp(timestamp / 1000000) <= '{end}'::TIMESTAMP
        GROUP BY time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000) )
        ORDER BY open_time ASC
        """
        self.con.execute(query)
        print(f"✓ Tabla ohlc_{interval_name} creada exitosamente")
        
    def close_connection(self):
        self.con.close()