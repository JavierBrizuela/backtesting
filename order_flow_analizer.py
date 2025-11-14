import duckdb
import pandas as pd

class OrderFlowAnalizer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.con = duckdb.connect(database=self.db_path)

    def get_ohlc(self, start, end, interval):
        query = f"""
        SELECT
            time_bucket(INTERVAL '{interval}', timestamp ) AS open_time,
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
                -- Vela verde con delta negativo = Distribución institucional
                -- WHEN close >= open AND delta < 0 THEN 'orange'
                
                -- Vela roja con delta positivo = Acumulación institucional  
                -- WHEN close < open AND delta > 0 THEN 'cyan'
                
                -- Vela verde alcista
                WHEN close >= open THEN 'green'
                
                -- Vela roja bajista
                ELSE 'red'
            END AS color_smart_money
        FROM agg_trade_history
        WHERE open_time >= '{start}' AND open_time <= '{end}'
        GROUP BY open_time
        ORDER BY open_time ASC
        """
        df_ohlc = self.con.execute(query).fetchdf()
        return df_ohlc

    def get_vol_profile(self, start, end, interval, resolution):
        query = f"""
        SELECT
            time_bucket('{interval}', timestamp) AS open_time,
            FLOOR(price/{resolution})*{resolution} AS price_bin,
            MAX(last_trade_id) - MIN(first_trade_id) + 1 AS trade_count,
            SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END) AS sell_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
            SUM(quantity) AS total_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE -quantity END) AS delta,
            delta::FLOAT / NULLIF(SUM(quantity), 0) AS delta_normalized,
            total_volume::FLOAT / MAX(total_volume) OVER () AS volume_normalized
        FROM agg_trade_history
        WHERE open_time >= '{start}' AND open_time <= '{end}'
        GROUP BY open_time, price_bin
        ORDER BY open_time ASC, price_bin ASC
        """
        df_vol_profile = self.con.execute(query).fetchdf()
        return df_vol_profile

    def fetch_order_flow_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = self.con.execute(query).fetchdf()
        return df

    def close_connection(self):
        self.con.close()
        