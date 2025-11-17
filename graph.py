from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter
from bokeh.layouts import column
import pandas as pd
import matplotlib
import numpy as np
from order_flow_analizer import OrderFlowAnalizer
from bokeh.models import LinearColorMapper
from bokeh.palettes import RdYlGn11, linear_palette

interval = '1 hours'
resolution = 50
start = '2025-09-25'
end = '2025-10-01'
simbol = 'BTCUSDT'
db_path = f'data/{simbol}/tradebook/agg_trades.db'
table = 'agg_trades'

db_connector = OrderFlowAnalizer(db_path)
df_ohlc = db_connector.get_ohlc(start, end, interval, table)
df_profile = db_connector.get_vol_profile(start, end, interval, table, resolution)
db_connector.close_connection()

width_ms = (df_ohlc['open_time'].iloc[1] - df_ohlc['open_time'].iloc[0]).total_seconds() * 1000
offset_ms = width_ms * 0.5
df_profile['bar_left'] = df_profile['open_time'] - pd.to_timedelta(offset_ms, unit='ms')
df_profile['bar_right'] = df_profile['bar_left'] + pd.to_timedelta(df_profile['volume_normalized'] * width_ms, unit='ms')
source_ohlc = ColumnDataSource(df_ohlc)
source_vol_profile = ColumnDataSource(df_profile)

plt_candlestick = figure(x_axis_type='datetime', width=1600, height=600)
candles = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='low', line_color='color_smart_money', source=source_ohlc, alpha=0.2)
plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color_smart_money', line_color=None, source=source_ohlc, legend_label=f'{simbol}-{interval} Candlesticks', alpha=0.2)
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
#plt_candlestick.xaxis.visible = False 
plt_candlestick.yaxis.formatter = NumeralTickFormatter(format="0,0.00")
# Crear un colormap personalizado
cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
    "red_white_green", ["#b50000", "#ffffff", "#007000"]
)

# Convertirlo a una lista de 256 hexadecimales para Bokeh
palette_256 = [matplotlib.colors.rgb2hex(cmap(i)) for i in np.linspace(0, 1, 256)]
# Configurar maximos y minimos para el LinearColorMapper
delta_min = df_profile['delta_normalized'].min()
delta_max = df_profile['delta_normalized'].max()
color_mapper = LinearColorMapper(
        palette=palette_256,
        low=delta_min,
        high=delta_max
    )
heatmap = plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution*0.9, line_color='black', line_alpha=0.3, color={'field': 'delta_normalized', 'transform': color_mapper}, source=source_vol_profile, alpha=0.4)
hover_heatmap = HoverTool(
    renderers=[heatmap],
    tooltips=[
        ("Tiempo", "@open_time{%F %T}"),
        ("Precio", "@price_bin{0,0.00}"),
        ("Volumen", "@total_volume{0,0.00}"),
        ("Volumen Normalizado", "@volume_normalized{0.00}"),
        ("Buy Volume", "@buy_volume{0,0.00}"),
        ("Sell Volume", "@sell_volume{0,0.00}"),
        ("Delta", "@delta{+0,0.00}"),
        ("Delta Normalized", "@delta_normalized{0.00}")
    ], formatters={'@open_time': 'datetime'})
df_poc = df_profile.loc[df_profile.groupby('open_time')['total_volume'].idxmax()][['open_time', 'price_bin', 'bar_left', 'bar_right', 'total_volume']].reset_index(drop=True).sort_values('open_time')
plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution, color=None, line_color='black', source=ColumnDataSource(df_poc))
plt_candlestick.add_tools(hover_heatmap)
plt_volume = figure(x_axis_type='datetime', width=1600, height=200, x_range=plt_candlestick.x_range)
window=20
df_ohlc['volume_ma'] = df_ohlc[ 'volume'].rolling(window=window).mean()
df_volume = df_ohlc.loc[df_ohlc['volume'] > df_ohlc['volume_ma'] * 1.5, ['open_time', 'volume']]
source_volume = ColumnDataSource(df_volume)
plt_volume.vbar_stack(stackers=['buy_volume', 'sell_volume'], x='open_time', width=width_ms, color=['red', 'green'], source=source_ohlc)
plt_volume.vbar(x='open_time', width=width_ms, top='volume', bottom=0, fill_color=None, line_color='black', source=source_volume)
show(column(plt_candlestick, plt_volume))