import duckdb
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.layouts import column

con = duckdb.connect(database='data/BTCUSDT/tradebook/trade_history.db')
df_ohlc = con.execute("""SELECT * FROM candles_4_hours""").fetchdf()
df_vol_profile = con.execute("""SELECT * FROM volume_profile_4_hours""").fetchdf()
con.close()
df_ohlc['color'] = ['green' if close >= open_ else 'red' for close, open_ in zip (df_ohlc['close'], df_ohlc['open'])]
df_vol_profile['delta'] = df_vol_profile['buy_volume'] - df_vol_profile['sell_volume']
source_ohlc = ColumnDataSource(df_ohlc)
plt_candlestick = figure(x_axis_type='datetime', title='BTCUSDT 4 Hours Candlesticks', width=1000, height=600)
plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='low', line_color='color', source=source_ohlc)
plt_candlestick.vbar(x='open_time', width=3.6e+6*4*0.8, top='open', bottom='close', fill_color='color', line_color=None, source=source_ohlc)
hover = HoverTool(
    tooltips=[
        ("Time", "@open_time{%F %T}"),
        ("Open", "@open{0.2f}"),
        ("High", "@high{0.2f}"),
        ("Low", "@low{0.2f}"),
        ("Close", "@close{0.2f}"),
        ("Volume", "@volume{0.00}"),
        ("Trade Count", "@trade_count")
    ],
    formatters={
        '@open_time': 'datetime',
    },
    mode='vline'
)
plt_candlestick.add_tools(hover)
plt_volume = figure(x_axis_type='datetime', title='BTCUSDT 4 Hours Volume', width=1000, height=200, x_range=plt_candlestick.x_range)
plt_volume.vbar(x=df_ohlc['open_time'], width=3.6e+6*4*0.8, top=df_ohlc['volume'], bottom=0, fill_color=df_ohlc['color'], line_color=None)
df_candle_vol = (
    df_vol_profile
    .groupby('open_time', as_index=False)[['buy_volume', 'sell_volume']]
    .sum()
    .sort_values('open_time')
)
print(df_candle_vol.head())
stackers = ['buy_volume', 'sell_volume']
source_vol_profile = ColumnDataSource(df_candle_vol)
width_ms = 4 * 60 * 60 * 1000 * 0.9
plt_vol_stack = figure(x_axis_type='datetime', title='BTCUSDT 4 Hours Delta', width=1000, height=200, x_range=plt_candlestick.x_range)
plt_vol_stack.vbar_stack(stackers, x='open_time',width=width_ms, fill_color = ['green', 'red'], line_color=None, source=source_vol_profile, legend_label=['Buy Volume', 'Sell Volume'])
plt_vol_stack.add_tools(HoverTool(
    tooltips=[
        ("Time", "@open_time{%F %T}"),
        ("Buy Volume", "@buy_volume{0.00}"),
        ("Sell Volume", "@sell_volume{0.00}"),
    ],
    formatters={
        '@open_time': 'datetime',
    }
))
show(column(plt_candlestick, plt_vol_stack))