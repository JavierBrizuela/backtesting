import duckdb

con = duckdb.connect(database='data/BTCUSDT/tradebook/trade_history.db')
result = con.execute("SELECT * FROM agg_trade_history").fetchdf()

print( result.tail() )