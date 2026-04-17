import duckdb

class AnalyticsDB:
    def __init__(self, db_path='analytics.db'):
        self.con = duckdb.connect(db_path)
    
    def create_date_query(self, start_date=None, end_date=None):
        # Filtros de fecha
        where_clauses = []
        if start_date:
            where_clauses.append(f"open_time >= '{start_date}'")
        if end_date:
            where_clauses.append(f"open_time <= '{end_date}'")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql
    
    def get_ohlc(self, interval, start_date=None, end_date=None):
        """
        Obtiene velas OHLC reagrupadas desde 1 minuto a cualquier intervalo.

        Columnas:
        open, high, low, close, volume, sell_volume, buy_volume, delta, trade_count
        delta_cumulative — suma corrida de delta
        color — 'green' si close >= open, 'red' si no
        """
        where_sql = self.create_date_query(start_date, end_date)
        
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
        return self.con.execute(query).fetchdf()
    
    def get_volume_profile(self, interval='15 minutes', start_date=None, end_date=None,
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

        where_sql = self.create_date_query(start_date, end_date)

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
        return self.con.execute(query).fetchdf()
    
    def get_profile(self, start_date=None, end_date=None, resolution=100):
        """
        Volume profile total acumulado del período (sin time buckets).
        Devuelve: price_bin, total_volume, total_volume_normalized
        """
        where_sql = self.create_date_query(start_date, end_date)
        
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
        return self.con.execute(query).fetchdf()
  
    def get_market_context(self, interval, start_date=None, end_date=None):
        
        where_sql = self.create_date_query(start_date, end_date)

        query = f"""
        WITH 
        ohlc AS (
        SELECT 
            time_bucket('{interval}', open_time) AS open_time,
            FIRST(open ORDER BY open_time ASC) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            FIRST(close ORDER BY open_time DESC) AS close,
        FROM ohlc_1_minutes
        {where_sql}
        GROUP BY 1
        ),
        base AS (
        SELECT
            *,
            ROW_NUMBER() OVER (ORDER BY open_time) AS row,
            GREATEST(
                high-low, 
                ABS(high-LAG(close) OVER (ORDER BY open_time)),
                ABS(low-LAG(close) OVER (ORDER BY open_time))
                ) AS TR,
        FROM ohlc
        ),
        atr AS (
        SELECT
            *,
            ROUND(
                AVG(TR) OVER (ORDER BY open_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
                6
            ) AS ATR_14
        from base
        ),
        swing_raw AS(
        SELECT
            *,
            COALESCE(
                high > LAG(high, 1) OVER (ORDER BY row)
                AND high > LAG(high, 2) OVER (ORDER BY row)
                AND high > LAG(high, 3) OVER (ORDER BY row)
                AND high > LEAD(high, 1) OVER (ORDER BY row)
                AND high > LEAD(high, 2) OVER (ORDER BY row)
                AND high > LEAD(high, 3) OVER (ORDER BY row),
                FALSE
            ) AS is_sh,
            COALESCE(
                low < LAG(low, 1) OVER (ORDER BY row)
                AND low < LAG(low, 2) OVER (ORDER BY row)
                AND low < LAG(low, 3) OVER (ORDER BY row)
                AND low < LEAD(low, 1) OVER (ORDER BY row)
                AND low < LEAD(low, 2) OVER (ORDER BY row)
                AND low < LEAD(low, 3) OVER (ORDER BY row),

                FALSE
            ) AS is_sl
        FROM atr
        ),
        sh_typed AS (
        SELECT
            *,
            CASE
                WHEN LAG(high) OVER(ORDER BY row) IS NULL THEN NULL
                WHEN high > LAG(high) OVER(ORDER BY row) THEN 'HH'
                ELSE 'LH'
            END AS sh_type
        FROM swing_raw
        WHERE is_sh
        ),
        sl_typed AS (
        SELECT
            *,
            CASE
                WHEN LAG(low) OVER(ORDER BY row) IS NULL THEN NULL
                WHEN low < LAG(low) OVER(ORDER BY row) THEN 'LL'
                ELSE 'HL'
            END AS sl_type
        FROM swing_raw
        WHERE is_sl
        ),
    -- ─────────────────────────────────────────────────────────────────────────
    -- BLOQUE 7 · Unir clasificaciones HH/LH/HL/LL a la tabla principal
    -- ─────────────────────────────────────────────────────────────────────────
        swings_classified AS (
            SELECT
                s.*,
                sh.sh_type,
                sl.sl_type
            FROM swing_raw s
            LEFT JOIN sh_typed sh ON s.row = sh.row
            LEFT JOIN sl_typed sl ON s.row = sl.row
        )
        SELECT
            *
        FROM swings_classified 
        WHERE is_sh OR is_sl
        ORDER BY open_time ASC
        """
        return self.con.execute(query).fetchdf()
    
    def close_connection(self):
        self.con.close()
        