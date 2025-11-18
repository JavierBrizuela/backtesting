import duckdb
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter
from bokeh.layouts import column
import pandas as pd

con = duckdb.connect(database='data/BTCUSDT/tradebook/trade_history.db')
df_ohlc = con.execute("""SELECT * FROM candles_4_hours""").fetchdf()
df_vol_profile = con.execute("""SELECT * FROM volume_profile_4_hours""").fetchdf()
con.close()

width_ms = 4 * 60 * 60 * 1000  # 4 hours in milliseconds
df_ohlc['color'] = ['green' if close >= open_ else 'red' for close, open_ in zip (df_ohlc['close'], df_ohlc['open'])]
df_vol_profile['delta'] = df_vol_profile['buy_volume'] - df_vol_profile['sell_volume']
df_vol_profile['buy_vol_norm'] = df_vol_profile['buy_volume'] / df_vol_profile['buy_volume'].max()
df_vol_profile['sell_vol_norm'] = df_vol_profile['sell_volume'] / df_vol_profile['sell_volume'].max()
df_vol_profile['buy_vol_bar'] = df_vol_profile['open_time'] + pd.to_timedelta( df_vol_profile['buy_vol_norm'] * width_ms, unit='ms')
df_vol_profile['sell_vol_bar'] = df_vol_profile['open_time'] - pd.to_timedelta(df_vol_profile['sell_vol_norm'] * width_ms, unit='ms')
# Candlestick Plot with Volume Profile
source_ohlc = ColumnDataSource(df_ohlc)
source_vol_profile = ColumnDataSource(df_vol_profile)
plt_candlestick = figure(x_axis_type='datetime', width=1200, height=560)
candles = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='low', line_color='color', source=source_ohlc, alpha=0.4)
plt_candlestick.vbar(x='open_time', width=3.6e+6*4*0.8, top='open', bottom='close', fill_color='color', line_color=None, source=source_ohlc, legend_label='BTCUSDT 4 Hours Candlesticks', alpha=0.4)

plt_candlestick.hbar(y='price_bin', left='open_time', right='buy_vol_bar', height=90, color='green', source=source_vol_profile)
plt_candlestick.hbar(y='price_bin', left='sell_vol_bar', right='open_time', height=90, color='red', source=source_vol_profile)
# calculate POC
df_vol_profile['total_volume'] = df_vol_profile['buy_volume'] + df_vol_profile['sell_volume']
df_poc = (
    df_vol_profile
    .loc[df_vol_profile.groupby('open_time')['total_volume'].idxmax()]
    [['open_time', 'price_bin', 'sell_vol_bar', 'buy_vol_bar', 'total_volume']]
    .reset_index(drop=True)
    .sort_values('open_time')
)
plt_candlestick.hbar(y='price_bin', left='sell_vol_bar', right='buy_vol_bar', height=100, color=None, line_color='black', source=ColumnDataSource(df_poc), legend_label='Point of Control (POC)')   
hover = HoverTool(
    renderers=[candles],
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
plt_candlestick.xaxis.visible = False
plt_candlestick.yaxis.formatter = NumeralTickFormatter(format="0,0.00")

# Volume Plot
plt_volume = figure(x_axis_type='datetime', width=1200, height=180, x_range=plt_candlestick.x_range)
plt_volume.vbar(x=df_ohlc['open_time'], width=3.6e+6*4*0.8, top=df_ohlc['volume'], bottom=0, fill_color=df_ohlc['color'], line_color=None, legend_label='BTCUSDT 4 Hours Volume' )

# Volume Delta Stack Plot
df_candle_vol = (
    df_vol_profile
    .groupby('open_time', as_index=False)[['buy_volume', 'sell_volume']]
    .sum()
    .sort_values('open_time')
)
stackers = ['sell_volume', 'buy_volume']
source_vol_stack = ColumnDataSource(df_candle_vol)
plt_vol_stack = figure(x_axis_type='datetime', width=1200, height=120, x_range=plt_candlestick.x_range)
plt_vol_stack.vbar_stack(stackers, x='open_time',width=width_ms, fill_color = ['red', 'green'], line_color=None, source=source_vol_stack, legend_label=['Sell Volume', 'Buy Volume'])
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