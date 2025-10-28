import duckdb

con = duckdb.connect(database='data/BTCUSDT/tradebook/trade_history.db')
result = con.execute("""
                    DESCRIBE TABLE candles_4_hours 
                    
                     """).fetchdf()

print( result )