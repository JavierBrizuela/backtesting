import analytics_db

interval = '4 hours'
start = '2026-02-01'
end = '2026-02-21'
simbol = 'BTCUSDT'
analytics_path = f'data/{simbol}/tradebook/analytics.db'
db = analytics_db.AnalyticsDB(analytics_path)
result = db.get_market_context(interval, start, end)
print(result)

db.close_connection()
