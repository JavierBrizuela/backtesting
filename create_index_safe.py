"""
Script para crear índice en tabla agg_trades con configuración optimizada de memoria.
Úsalo SOLO si realmente necesitas el índice (no recomendado para DuckDB columnar).
"""
import duckdb
import sys

def create_index_with_limited_memory():
    print("[INFO] Conectando a raw_data.db...")

    # Conectar sin cargar nada en memoria
    con = duckdb.connect('data/BTCUSDT/tradebook/raw_data.db', read_only=False)

    # Configuración CRÍTICA para tablas grandes
    print("[INFO] Configurando límites de memoria...")
    con.execute("SET memory_limit = '5GB'")
    con.execute("SET threads = 2")  # Reducir threads para menos consumo de memoria
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET enable_progress_bar = true")

    # Verificar si el índice ya existe
    existing = con.execute("""
        SELECT index_name
        FROM duckdb_indexes()
        WHERE table_name = 'agg_trades' AND column_name = 'timestamp'
    """).fetchone()

    if existing:
        print(f"[OK] El índice {existing[0]} ya existe.")
        con.close()
        return

    print("[INFO] Creando índice idx_agg_trades_timestamp...")
    print("[WARN] Esto puede tardar 30-60 minutos en una tabla de 14GB")
    print("[WARN] No interrumpas el proceso o la base de datos puede corromperse")

    try:
        # Crear índice con timeout extendido
        con.execute('''
            CREATE INDEX idx_agg_trades_timestamp ON agg_trades(timestamp)
        ''')
        print("[OK] Índice creado exitosamente")
    except Exception as e:
        print(f"[ERROR] Falló la creación del índice: {e}")
        print("[INFO] Sugerencia: Aumenta el memory_limit o usa sin índice")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    create_index_with_limited_memory()
