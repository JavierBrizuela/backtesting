import duckdb

class AggTradeDB:
    def __init__(self, db_path, UTC=False):
        self.db_path = db_path
        self.con = duckdb.connect(database=self.db_path)
        if UTC:
            self.con.execute("SET TimeZone='UTC'")
    
    def table_exists(self, table):
        res = self.con.execute(f'''
            SELECT COUNT(*) AS count
            FROM information_schema.tables
            WHERE table_name = '{table}'
        ''').fetchone()
        return res[0] > 0
    
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
        
    def create_ohlc_table(self, interval):
        interval_name = interval.replace(" ", "_")
        table_exists = self.table_exists(f"ohlc_{interval_name}")
        if not table_exists:
            self.con.execute(f"""
                CREATE TABLE ohlc_{interval_name} (
                    open_time TIMESTAMP PRIMARY KEY,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    sell_volume DOUBLE,
                    buy_volume DOUBLE,
                    volume DOUBLE,
                    delta DOUBLE,
                    trade_count BIGINT,
                    color VARCHAR
                )
            """)
            print(f"✓ Tabla ohlc_{interval_name} creada exitosamente")
            where_clause = ""
        else:
            where_clause = f"WHERE to_timestamp(timestamp / 1000000) > (SELECT MAX(open_time) FROM ohlc_{interval_name})"
            print(f"✓ Tabla ohlc_{interval_name} se actualiza con nuevos datos")
            
        query = f"""
        INSERT OR IGNORE INTO ohlc_{interval_name}
        SELECT
            CAST(time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000)) AS TIMESTAMP) AS open_time,
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
            END AS color
        FROM agg_trades
        {where_clause}
        GROUP BY time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000) )
        ORDER BY open_time ASC
        """
        self.con.execute(query)
    
    def create_volume_profile_table(self, resolution, interval):
        table_exists = self.table_exists("volume_profile")
        if not table_exists:
            self.con.execute("""
                CREATE TABLE volume_profile (
                    open_time TIMESTAMP,
                    price_bin DOUBLE,
                    trade_count BIGINT,
                    sell_volume DOUBLE,
                    buy_volume DOUBLE,
                    total_volume DOUBLE,
                    delta DOUBLE,
                    PRIMARY KEY (open_time, price_bin)
                )
                """)
            print("✓ Tabla volume_profile creada exitosamente")
            where_clause = ""
        else:
            where_clause = f"WHERE to_timestamp(timestamp / 1000000) > (SELECT MAX(open_time) FROM volume_profile)"
            print("✓ Tabla volume_profile se actualiza con nuevos datos")
        query = f"""
        INSERT OR IGNORE INTO volume_profile
        SELECT
            CAST(time_bucket(INTERVAL '{interval}' , to_timestamp(timestamp/1000000)) AS TIMESTAMP) AS open_time,
            FLOOR(price/{resolution})*{resolution} AS price_bin,
            MAX(last_trade_id) - MIN(first_trade_id) + 1 AS trade_count,
            SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END) AS sell_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
            SUM(quantity) AS total_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE -quantity END) AS delta,
        FROM agg_trades
        {where_clause}
        GROUP BY time_bucket(INTERVAL '{interval}' , to_timestamp(timestamp/1000000)), FLOOR(price/{resolution})*{resolution}
        ORDER BY open_time ASC, price_bin ASC
        """
        self.con.execute(query)
    
    def get_ohlc(self, interval, start_date=None, end_date=None):
        
         # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"""
        WITH resampled AS (
        SELECT
            time_bucket(INTERVAL '{interval}', open_time) AS open_time,
            FIRST(open ORDER BY open_time ASC) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            FIRST(close ORDER BY open_time DESC) AS close,
            SUM(sell_volume) AS sell_volume,
            SUM(buy_volume) AS buy_volume,
            SUM(volume) AS volume,
            SUM(delta) AS delta,
            SUM(trade_count) AS trade_count
        FROM ohlc_1_minutes
        {where_sql}
        GROUP BY time_bucket(INTERVAL '{interval}', open_time)
    )
    SELECT
        open_time,
        open,
        high,
        low,
        close,
        sell_volume,
        buy_volume,
        volume,
        delta,
        trade_count,
        CASE 
            WHEN close >= open THEN 'green'
            ELSE 'red'
        END AS color
    FROM resampled
    ORDER BY open_time ASC
    """
        df = self.con.execute(query).fetchdf()
        return df
    
    def get_volume_profile(self, interval='4 hours', start_date=None, end_date=None, 
                           resolution='auto'):
        """
        Calcula volume profile desde tabla base de 1m
        Reagrupa bins según la resolución solicitada
        y el intervalo de tiempo
        Args:
            interval: '5 minutes', '15 minutes', '1 hour', '4 hours', '1 day'
            start_date: Filtro fecha inicio
            end_date: Filtro fecha fin
            resolution: 'auto' o valor numérico (10, 25, 50, 100, 200, 500, 1000)
        
        Returns:
            DataFrame con volume profile
        """
        interval_name = interval.replace(" ", "_")
        if resolution == 'auto':
            if interval in ['1 minutes', '5 minutes', '15 minutes']:
                resolution = 10
            elif interval == '1 hour':
                resolution = 25
            elif interval == '4 hours':
                resolution = 50
            elif interval == '1 day':
                resolution = 100
            else:
                resolution = 10  # Default fallback
        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        # Consulta principal
        query = f"""
            WITH resampled AS (
                SELECT
                    time_bucket(INTERVAL '{interval}', open_time) AS open_time,
                    FLOOR(price_bin / {resolution}) * {resolution} AS price_bin,
                    SUM(trade_count) AS trade_count,
                    SUM(sell_volume) AS sell_volume,
                    SUM(buy_volume) AS buy_volume,
                    SUM(total_volume) AS total_volume,
                    SUM(delta) AS delta
                FROM volume_profile
                {where_sql}
                GROUP BY time_bucket(INTERVAL '{interval}', open_time), FLOOR(price_bin / {resolution}) * {resolution}
            ),
            normalized AS (
                SELECT
                    *,
                    -- NORMALIZACIONES LOCALES
                    total_volume::FLOAT / NULLIF(
                        MAX(total_volume) OVER (PARTITION BY open_time), 0
                    ) AS volume_local_normalized,
                    
                    CASE 
                        WHEN delta >= 0 THEN 
                            delta::FLOAT / NULLIF(MAX(delta) OVER (PARTITION BY open_time), 0)
                        ELSE 
                            delta::FLOAT / NULLIF(ABS(MIN(delta) OVER (PARTITION BY open_time)), 0)
                    END AS delta_local_normalized,
                    
                    -- NORMALIZACIONES GLOBALES
                    total_volume::FLOAT / NULLIF(
                        MAX(total_volume) OVER (), 0
                    ) AS volume_global_normalized,
                    
                    CASE 
                        WHEN delta >= 0 THEN 
                            delta::FLOAT / NULLIF(MAX(delta) OVER (), 0)
                        ELSE 
                            delta::FLOAT / NULLIF(ABS(MIN(delta) OVER ()), 0)
                    END AS delta_global_normalized
                FROM resampled
            )
            SELECT * FROM normalized
            ORDER BY open_time ASC, price_bin ASC
            """
        df = self.con.execute(query).fetchdf()
        return df
    
    def close_connection(self):
        self.con.close()