from agg_trades_to_db import AggTradeDB
import os

raw_path = 'data/BTCUSDT/tradebook/raw_data.db'
analytics_path = 'data/BTCUSDT/tradebook/analytics.db'

print("\n--- INICIANDO TEST DE INTEGRIDAD ---")
try:
    # 1. Instanciar DB (conexión dual)
    db = AggTradeDB(raw_path, analytics_path)
    print("[OK] Conexión establecida con raw y analytics.")

    # 2. Verificar tablas en cada esquema
    print("\n[VERIFICACIÓN] Tablas en MAIN (raw_data.db):")
    main_tables = db.con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    print([t[0] for t in main_tables])

    print("\n[VERIFICACIÓN] Tablas en ANALYTICS (analytics.db):")
    analytics_tables = db.con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'").fetchall()
    print([t[0] for t in analytics_tables])

    # 3. Intentar generar OHLC (Poblar ohlc_1_minutes que estaba vacía)
    print("\n[PROCESANDO] Generando datos para ohlc_1_minutes (pueden ser muchas filas)...")
    db.create_ohlc_table_from_aggtrades('1 minutes')
    
    # 4. Comprobar si ahora tiene datos
    count = db.con.execute("SELECT COUNT(*) FROM analytics.ohlc_1_minutes").fetchone()[0]
    print(f"[OK] ohlc_1_minutes ahora tiene {count:,} filas.")

    # 5. Cerrar conexión
    db.close_connection()
    print("\n[FIN] Test completado exitosamente.")

except Exception as e:
    print(f"\n[ERROR] Fallo en el test de integridad: {e}")
