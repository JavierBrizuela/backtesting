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
                timestamp TIMESTAMP,
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
        
    def close_connection(self):
        self.con.close()