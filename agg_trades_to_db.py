import duckdb
import pandas as pd
import time
from datetime import datetime
class AggTradeDB:
    def __init__(self, raw_path, analytics_path, UTC=False, read_only=False):
        self.raw_path = raw_path
        self.analytics_path = analytics_path
        # Conectamos a raw_data.db como base de datos principal
        self.con = duckdb.connect(database=self.raw_path, read_only=read_only)

        # Configuración de memoria para evitar OutOfMemory en tablas grandes
        if not read_only:
            self.con.execute("SET memory_limit = '5GB'")      # Dejar 1GB para el sistema
            self.con.execute("SET threads = 4")              # Limitar paralelismo
            self.con.execute("SET preserve_insertion_order = false")  # Menor uso de memoria

        # Atacheamos analytics.db para métricas procesadas
        self.con.execute(f"ATTACH '{self.analytics_path}' AS analytics (READ_ONLY {str(read_only).upper()})")
        # UTC false para que use la zona horaria local del sistema, true para forzar UTC
        if UTC:
            self.con.execute("SET TimeZone='UTC'")

    def table_exists(self, table, schema='analytics'):
        result = self.con.execute(f"SHOW TABLES FROM {schema}").fetchdf()
        return table in result['name'].values

    def str_to_timestamp(self, date_str):
        return int(pd.Timestamp(date_str).value // 1000)

    def create_ohlc_table(self, interval):
        try:
            self.con.execute(f"""
                CREATE TABLE IF NOT EXISTS analytics.ohlc_{interval} (
                    open_time TIMESTAMPTZ PRIMARY KEY,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    sell_volume DOUBLE,
                    buy_volume DOUBLE,
                    volume DOUBLE,
                    delta DOUBLE,
                    trade_count BIGINT
                )
            """)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.ohlc_{interval} preparada")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Tabla analytics.ohlc_{interval}: {e}")
            raise

    def create_raw_table(self):
        try:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS agg_trades (
                    agg_trade_id BIGINT PRIMARY KEY,
                    price DOUBLE,
                    quantity DOUBLE,
                    first_trade_id BIGINT,
                    last_trade_id BIGINT,
                    timestamp BIGINT,
                    is_buyer_maker BOOLEAN,
                    is_best_match BOOLEAN
                )
            """)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla agg_trades preparada")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Tabla agg_trades: {e}")
            raise
        
    def import_csv_to_db(self, csv_path, table):
        """
        Importa un archivo CSV directamente a DuckDB (main) sin pasar por la RAM de Python.
        """
        if not csv_path:
            return None

        start_time = time.time()
        csv_path_sql = csv_path.replace("\\", "/")

        # Creamos la tabla si no existe en main
        table_exists = self.table_exists(table, schema='main')
        if not table_exists:
            self.create_raw_table()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [SQL] Importando datos desde CSV a {table}...")
        count_before = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        self.con.execute(f'''
            INSERT OR IGNORE INTO {table}
            SELECT * FROM read_csv_auto('{csv_path_sql}', header=False)
            ORDER BY column5 ASC
        ''')

        count_after = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Importación terminada: {count_after - count_before} registros añadidos ({elapsed:.1f}s)")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Total registros en tabla '{table}': {count_after:,}")

    def save_df_to_db(self, df, table):
        if df.empty:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [WARN] No hay datos en el dataframe para crear la tabla.")
            return None

        start_time = time.time()
        # Ordenar por timestamp antes de insertar
        df = df.sort_values("timestamp")

        # Creamos la tabla si no existe en main
        table_exists = self.table_exists(table, schema='main')
        if not table_exists:
            self.create_raw_table()
            
        count_before = self.con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
        # Ordenar por timestamp (columna 5) para mejor compresión
        self.con.execute(f'''
            INSERT OR IGNORE INTO {table}
            SELECT * FROM df ORDER BY timestamp ASC
        ''')
        count_after = self.con.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()[0]
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Registros antes: {count_before}, después: {count_after}, añadidos: {count_after - count_before} ({elapsed:.1f}s)")

    def create_ohlc_table_from_aggtrades(self, interval):
        interval_name = interval.replace(" ", "_")
        start_time = time.time()
        table_exists = self.table_exists(f"ohlc_{interval_name}")
        if not table_exists:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.ohlc_{interval_name} no existe, se creará y procesará todo desde el inicio.")
            self.create_ohlc_table(interval_name)
            where_clause = ""
        else:
            # Optimización: Obtener el timestamp máximo primero y usarlo directamente
            max_timestamp = self.con.execute(f"""
                SELECT CAST(epoch(MAX(open_time)) * 1000000 AS BIGINT)
                FROM analytics.ohlc_{interval_name}
            """).fetchone()[0]
            if max_timestamp is None:
                where_clause = ""
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.ohlc_{interval_name} existe pero vacía, procesando todo")
            else:
                where_clause = f"WHERE timestamp > {max_timestamp}"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.ohlc_{interval_name} se actualiza desde timestamp {max_timestamp}")

        query = f"""
        INSERT OR IGNORE INTO analytics.ohlc_{interval_name}
        SELECT
            time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000)) AS open_time,
            FIRST(price ORDER BY timestamp ASC) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            FIRST(price ORDER BY timestamp DESC) AS close,
            SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END) AS sell_volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
            SUM(quantity) AS volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE -quantity END) AS delta,
            MAX(last_trade_id) - MIN(first_trade_id) + 1 AS trade_count
        FROM agg_trades
        {where_clause}
        GROUP BY time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000))
        ORDER BY open_time ASC
        """
        self.con.execute(query)
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.ohlc_{interval_name} actualizada ({elapsed:.1f}s)")

    def create_volume_profile_table(self, resolution, interval):
        start_time = time.time()
        table_exists = self.table_exists("volume_profile")
        if not table_exists:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS analytics.volume_profile (
                    open_time TIMESTAMPTZ,
                    price_bin DOUBLE,
                    trade_count BIGINT,
                    sell_volume DOUBLE,
                    buy_volume DOUBLE,
                    total_volume DOUBLE,
                    delta DOUBLE,
                    PRIMARY KEY (open_time, price_bin)
                )
                """)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.volume_profile preparada (Res: {resolution})")
            where_clause = ""
        else:
            # Optimización: Obtener el timestamp máximo primero y usarlo directamente
            max_timestamp = self.con.execute("""
                SELECT CAST(epoch(MAX(open_time)) * 1000000 AS BIGINT)
                FROM analytics.volume_profile
            """).fetchone()[0]

            if max_timestamp is None:
                where_clause = ""
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.volume_profile existe pero vacia, procesando todo")
            else:
                where_clause = f"WHERE timestamp > {max_timestamp}"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.volume_profile se actualiza desde timestamp {max_timestamp}")
        query = f"""
        INSERT OR IGNORE INTO analytics.volume_profile
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
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.volume_profile actualizada ({elapsed:.1f}s)")

    def create_market_context_table(self, interval):
        start_time = time.time()
        interval_name = interval.replace(" ", "_")
        table_exists = self.table_exists(f"market_context_{interval_name}")

        if not table_exists:
            # Nueva tabla con columnas de Market Structure
            self.con.execute(f"""
                CREATE TABLE IF NOT EXISTS analytics.market_context_{interval_name} (
                    open_time TIMESTAMPTZ PRIMARY KEY,
                    efficiency DOUBLE,
                    efficiency_ratio DOUBLE,
                    r_squared DOUBLE,
                    coefficient_variation DOUBLE,
                    atr_normalized DOUBLE,
                    delta_efficiency DOUBLE,
                    regime TEXT DEFAULT NULL,
                    price_location TEXT DEFAULT NULL,
                    -- Market Structure columns
                    last_swing_high DOUBLE,
                    last_swing_low DOUBLE,
                    swing_high_time TIMESTAMP,
                    swing_low_time TIMESTAMP,
                    market_structure_event TEXT DEFAULT NULL,
                    trend_direction TEXT DEFAULT NULL,
                    bars_since_structure INTEGER
                )
            """)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.market_context_{interval_name} creada con Market Structure")
            where_clause = ""
        else:
            # Optimización: Obtener timestamp máximo primero (open_time ya es TIMESTAMP)
            max_open_time = self.con.execute(f"""
                SELECT MAX(open_time)
                FROM analytics.market_context_{interval_name}
            """).fetchone()[0]

            if max_open_time is None:
                where_clause = ""
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.market_context_{interval_name} existe pero vacia, procesando todo")
            else:
                where_clause = f"WHERE open_time > '{max_open_time}'"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.market_context_{interval_name} se actualiza desde {max_open_time}")
        
        # Query completo con Market Structure detection
        query = f"""
        WITH ohlc AS (
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
            FROM analytics.ohlc_1_minutes
            {where_clause}
            GROUP BY time_bucket(INTERVAL '{interval}', open_time)
            ),
        base_data AS (
            SELECT
                *,
                LAG(close) OVER (ORDER BY open_time) AS prev_close,
                ROW_NUMBER() OVER (ORDER BY open_time) AS row_number
            FROM ohlc
        ),
        -- Detectar Swing Highs (máximo en ventana de 5 velas a cada lado)
        swing_highs AS (
            SELECT
                open_time,
                high,
                CASE
                    WHEN high = MAX(high) OVER (ORDER BY open_time ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING)
                        AND high > LAG(high, 1) OVER (ORDER BY open_time)
                        AND high > LAG(high, 2) OVER (ORDER BY open_time)
                        AND high > LAG(high, 3) OVER (ORDER BY open_time)
                        AND high > LAG(high, 4) OVER (ORDER BY open_time)
                        AND high > LAG(high, 5) OVER (ORDER BY open_time)
                        AND high > LEAD(high, 1) OVER (ORDER BY open_time)
                        AND high > LEAD(high, 2) OVER (ORDER BY open_time)
                        AND high > LEAD(high, 3) OVER (ORDER BY open_time)
                        AND high > LEAD(high, 4) OVER (ORDER BY open_time)
                        AND high > LEAD(high, 5) OVER (ORDER BY open_time)
                    THEN high
                    ELSE NULL
                END AS is_swing_high
            FROM base_data
        ),
        -- Detectar Swing Lows (mínimo en ventana de 5 velas a cada lado)
        swing_lows AS (
            SELECT
                open_time,
                low,
                CASE
                    WHEN low = MIN(low) OVER (ORDER BY open_time ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING)
                        AND low < LAG(low, 1) OVER (ORDER BY open_time)
                        AND low < LAG(low, 2) OVER (ORDER BY open_time)
                        AND low < LAG(low, 3) OVER (ORDER BY open_time)
                        AND low < LAG(low, 4) OVER (ORDER BY open_time)
                        AND low < LAG(low, 5) OVER (ORDER BY open_time)
                        AND low < LEAD(low, 1) OVER (ORDER BY open_time)
                        AND low < LEAD(low, 2) OVER (ORDER BY open_time)
                        AND low < LEAD(low, 3) OVER (ORDER BY open_time)
                        AND low < LEAD(low, 4) OVER (ORDER BY open_time)
                        AND low < LEAD(low, 5) OVER (ORDER BY open_time)
                    THEN low
                    ELSE NULL
                END AS is_swing_low
            FROM base_data
        ),
        -- Combinar datos base con swings
        swings_combined AS (
            SELECT
                b.*,
                sh.is_swing_high,
                sl.is_swing_low
            FROM base_data b
            LEFT JOIN swing_highs sh ON b.open_time = sh.open_time
            LEFT JOIN swing_lows sl ON b.open_time = sl.open_time
        ),
        -- Forward fill de swings usando LAST_VALUE IGNORE NULLS (mucho más rápido en DuckDB)
        with_swings AS (
            SELECT
                *,
                LAST_VALUE(is_swing_high IGNORE NULLS) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_sh,
                LAST_VALUE(is_swing_low IGNORE NULLS) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_sl,
                LAST_VALUE(CASE WHEN is_swing_high IS NOT NULL THEN open_time END IGNORE NULLS) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_sh_time,
                LAST_VALUE(CASE WHEN is_swing_low IS NOT NULL THEN open_time END IGNORE NULLS) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_sl_time
            FROM swings_combined
        ),
        with_prev_values AS (
            SELECT
                *,
                LAG(last_sh) OVER (ORDER BY open_time) as prev_sh,
                LAG(last_sl) OVER (ORDER BY open_time) as prev_sl
            FROM with_swings
        ),
        -- Detectar estructura de mercado
        structure_detection AS (
            SELECT
                *,
                -- Detectar tipo de estructura
                CASE
                    -- Nuevo Swing High
                    WHEN is_swing_high IS NOT NULL THEN
                        CASE
                            WHEN prev_sh IS NOT NULL AND is_swing_high > prev_sh THEN 'HH'
                            WHEN prev_sh IS NOT NULL AND is_swing_high <= prev_sh THEN 'LH'
                            ELSE 'SH_FIRST'
                        END
                    -- Nuevo Swing Low
                    WHEN is_swing_low IS NOT NULL THEN
                        CASE
                            WHEN prev_sl IS NOT NULL AND is_swing_low > prev_sl THEN 'HL'
                            WHEN prev_sl IS NOT NULL AND is_swing_low <= prev_sl THEN 'LL'
                            ELSE 'SL_FIRST'
                        END
                    -- Break of Structure (BOS)
                    WHEN close > last_sh AND last_sh IS NOT NULL THEN 'BOS_UP'
                    WHEN close < last_sl AND last_sl IS NOT NULL THEN 'BOS_DOWN'
                    ELSE NULL
                END AS structure_event,
                -- Determinar cambios en dirección de tendencia
                CASE
                    WHEN (last_sh > prev_sh AND last_sl >= prev_sl) OR structure_event = 'BOS_UP' THEN 'UPTREND'
                    WHEN (last_sl < prev_sl AND last_sh <= prev_sh) OR structure_event = 'BOS_DOWN' THEN 'DOWNTREND'
                    WHEN (last_sh < prev_sh AND last_sl > prev_sl) OR (last_sh > prev_sh AND last_sl < prev_sl) THEN 'RANGING'
                    ELSE NULL
                END AS trend_change
            FROM with_prev_values
        ),
        -- Hacer la tendencia persistente (Sticky Trend)
        with_sticky_trend AS (
            SELECT
                *,
                LAST_VALUE(trend_change IGNORE NULLS) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as trend_dir_persistent
            FROM structure_detection
        ),
        -- Calcular CHoCH (Change of Character) simplificado
        with_choch AS (
            SELECT
                *,
                COALESCE(trend_dir_persistent, 'UNDEFINED') as trend_dir,
                -- Simplificado: solo marcamos si hay cambio de tendencia claro
                CASE
                    WHEN structure_event IN ('BOS_UP', 'HH') AND trend_dir_persistent = 'UPTREND'
                        AND LAG(trend_dir_persistent) OVER (ORDER BY open_time) = 'DOWNTREND'
                    THEN 'CHoCH_UP'
                    WHEN structure_event IN ('BOS_DOWN', 'LL') AND trend_dir_persistent = 'DOWNTREND'
                        AND LAG(trend_dir_persistent) OVER (ORDER BY open_time) = 'UPTREND'
                    THEN 'CHoCH_DOWN'
                    ELSE structure_event
                END AS final_structure_event
            FROM with_sticky_trend
        )
        INSERT OR IGNORE INTO analytics.market_context_{interval_name}
        SELECT
            open_time,
            -- Métricas originales
            ABS(close - open) / NULLIF(volume, 0) AS efficiency,
            ABS(close - FIRST_VALUE(close) OVER w20) / NULLIF(
                SUM(ABS(close - prev_close)) OVER w20, 0
            ) AS efficiency_ratio,
            POW(CORR(row_number, close) OVER w20, 2) AS r_squared,
            STDDEV(close) OVER w20 / NULLIF(AVG(close) OVER w20, 0) AS coefficient_variation,
            AVG(GREATEST(high - low, ABS(high - prev_close), ABS(low - prev_close))) OVER w20
                / NULLIF(AVG(close) OVER w20, 0) AS atr_normalized,
            SUM(delta) OVER w20 / NULLIF(
                ABS(FIRST_VALUE(open) OVER w20 - LAST_VALUE(close) OVER w20), 0
            ) AS delta_efficiency,
            NULL AS regime,
            NULL AS price_location,
            -- Market Structure
            last_sh AS last_swing_high,
            last_sl AS last_swing_low,
            last_sh_time AS swing_high_time,
            last_sl_time AS swing_low_time,
            final_structure_event AS market_structure_event,
            trend_dir AS trend_direction,
            -- Velas desde último evento (simplificado)
            0::INTEGER AS bars_since_structure
        FROM with_choch
        WINDOW
            w20 AS (ORDER BY open_time ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
        ORDER BY open_time;
        """
        self.con.execute(query)
        elapsed = time.time() - start_time
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] Tabla analytics.market_context_{interval_name} actualizada con Market Structure ({elapsed:.1f}s)")

    def get_ohlc(self, interval, start_date=None, end_date=None):
        """
        Obtiene velas OHLC reagrupadas desde 1 minuto a cualquier intervalo.

        Columnas:
        open, high, low, close, volume, sell_volume, buy_volume, delta, trade_count
        delta_cumulative — suma corrida de delta
        color — 'green' si close >= open, 'red' si no
        """
        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        WITH ohlc_resampled AS (
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
            FROM analytics.ohlc_1_minutes
            {where_sql}
            GROUP BY 1
        )
        SELECT
            open_time, open, high, low, close,
            sell_volume, buy_volume, volume, delta, trade_count,
            SUM(delta) OVER (ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS delta_cumulative,
            CASE WHEN close >= open THEN 'green' ELSE 'red' END AS color
        FROM ohlc_resampled
        ORDER BY open_time ASC
        """
        df = self.con.execute(query).fetchdf()
        return df

    def get_volume_profile(self, interval='4 hours', start_date=None, end_date=None,
                           resolution='auto'):
        """
        Obtiene volume profile reagrupado desde 1m a cualquier intervalo.

        Columnas por price_bin:
        open_time, price_bin, trade_count, sell_volume, buy_volume, total_volume, delta
        is_poc, volume_local_normalized, volume_global_normalized
        delta_local_normalized, delta_global_normalized
        vwap, vwap_std — VWAP ponderado por volumen del perfil (estimado por bins)
        node_type — 'POC' | 'HVN' (dentro del Value Area) | 'LVN' (fuera del Value Area)
        value_area_low, value_area_high — límites del Value Area (70% del volumen)
        """
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
                resolution = 10

        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        half_res = resolution / 2
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
            FROM analytics.volume_profile
            {where_sql}
            GROUP BY 1, 2
        ),
        normalized AS (
            SELECT
                *,
                total_volume::FLOAT / NULLIF(MAX(total_volume) OVER (PARTITION BY open_time), 0) AS volume_local_normalized,
                CASE
                    WHEN delta >= 0 THEN delta::FLOAT / NULLIF(MAX(delta) OVER (PARTITION BY open_time), 0)
                    ELSE delta::FLOAT / NULLIF(ABS(MIN(delta) OVER (PARTITION BY open_time)), 0)
                END AS delta_local_normalized,
                total_volume::FLOAT / NULLIF(MAX(total_volume) OVER (), 0) AS volume_global_normalized,
                CASE
                    WHEN delta >= 0 THEN delta::FLOAT / NULLIF(MAX(delta) OVER (), 0)
                    ELSE delta::FLOAT / NULLIF(ABS(MIN(delta) OVER ()), 0)
                END AS delta_global_normalized
            FROM resampled
        ),
        vwap_calc AS (
            SELECT
                open_time,
                SUM((price_bin + {half_res}) * total_volume) / NULLIF(SUM(total_volume), 0) AS vwap,
                SQRT(GREATEST(0,
                    SUM(total_volume * POWER((price_bin + {half_res}), 2)) / NULLIF(SUM(total_volume), 0)
                    - POWER(SUM((price_bin + {half_res}) * total_volume) / NULLIF(SUM(total_volume), 0), 2)
                )) AS vwap_std
            FROM normalized
            GROUP BY open_time
        ),
        -- Value Area: bins ordenados por volumen desc, acumular 70% del total
        running AS (
            SELECT
                *,
                SUM(total_volume) OVER (
                    PARTITION BY open_time
                    ORDER BY total_volume DESC, price_bin DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_vol,
                SUM(total_volume) OVER (PARTITION BY open_time) AS total_vol
            FROM normalized
        ),
        -- POC y límites del Value Area (VAL=precio más bajo del VA, VAH=precio más alto del VA)
        va AS (
            SELECT
                open_time,
                total_vol,
                MAX(CASE WHEN running_vol - total_volume = 0 THEN price_bin END) AS poc,
                MIN(CASE WHEN running_vol - total_volume <= total_vol * 0.7 THEN price_bin END) AS val,
                MAX(CASE WHEN running_vol - total_volume <= total_vol * 0.7 THEN price_bin END) AS vah
            FROM running
            GROUP BY open_time, total_vol
        )
        SELECT
            n.open_time,
            n.price_bin,
            n.trade_count,
            n.sell_volume,
            n.buy_volume,
            n.total_volume,
            n.delta,
            n.volume_local_normalized,
            n.volume_global_normalized,
            n.delta_local_normalized,
            n.delta_global_normalized,
            v.vwap,
            v.vwap_std,
            CASE WHEN n.price_bin = a.poc THEN TRUE ELSE FALSE END AS is_poc,
            a.val AS value_area_low,
            a.vah AS value_area_high,
            CASE
                WHEN n.price_bin = a.poc THEN 'POC'
                WHEN n.price_bin BETWEEN a.val AND a.vah THEN 'HVN'
                ELSE 'LVN'
            END AS node_type
        FROM normalized n
        LEFT JOIN vwap_calc v USING (open_time)
        LEFT JOIN va a USING (open_time)
        ORDER BY n.open_time ASC, n.price_bin ASC
        """
        df = self.con.execute(query).fetchdf()
        return df

    def get_profile(self, start_date=None, end_date=None, resolution='auto'):
        """
        Volume profile total acumulado del período (sin time buckets).
        Devuelve: price_bin, total_volume, total_volume_normalized
        """
        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"""
            WITH resume AS(
                SELECT
                    FLOOR(price_bin / {resolution}) * {resolution} AS price_bin,
                    SUM(total_volume) AS total_volume
                FROM analytics.volume_profile
                {where_sql}
                GROUP BY FLOOR(price_bin / {resolution}) * {resolution}
            ),
            normalized AS (
                SELECT
                    price_bin,
                    total_volume,
                    total_volume::FLOAT / NULLIF(MAX(total_volume) OVER(), 0) AS total_volume_normalized
                FROM resume
            )
            SELECT * FROM normalized
            ORDER BY price_bin ASC
        """
        df = self.con.execute(query).fetchdf()
        return df

    def get_institutional_trades(self, start_date=None, end_date=None, interval='1 minute'):
        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"timestamp >= {self.str_to_timestamp(start_date)}")
        if end_date:
            where_clauses.append(f"timestamp <= {self.str_to_timestamp(end_date)}")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"""
            SELECT
                CAST(time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000)) AS TIMESTAMP) AS interval_time,
                quantity,
                AVG(price) AS avg_price,
                is_buyer_maker,
                COUNT(*) AS trade_count
            FROM agg_trades
            {where_sql}
            GROUP BY time_bucket(INTERVAL '{interval}', to_timestamp(timestamp / 1000000)), quantity, is_buyer_maker
            HAVING trade_count > 5
            ORDER BY trade_count DESC
        """

        df = self.con.execute(query).fetchdf()
        return df

    def close_connection(self):
        self.con.close()
