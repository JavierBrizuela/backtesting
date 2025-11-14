from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter
from bokeh.layouts import column
import pandas as pd
import matplotlib
import numpy as np
from order_flow_analizer import OrderFlowAnalizer
from bokeh.models import LinearColorMapper
from bokeh.palettes import RdYlGn11, linear_palette

interval = '4 hours'
resolution = 50
start = '2025-09-01'
end = '2025-09-15'
db_path = 'data/BTCUSDT/tradebook/trade_history.db'

db_connector = OrderFlowAnalizer(db_path)
df_ohlc = db_connector.get_ohlc(start, end, interval)
df_profile = db_connector.get_vol_profile(start, end, interval, resolution)
db_connector.close_connection()
print(df_ohlc.head())
width_ms = (df_ohlc['open_time'].iloc[1] - df_ohlc['open_time'].iloc[0]).total_seconds() * 1000
offset_ms = width_ms * 0.5
df_profile['bar_right'] = df_profile['open_time'] + pd.to_timedelta(df_profile['volume_normalized'] * width_ms, unit='ms')
df_profile['bar_left'] = df_profile['open_time'] - pd.to_timedelta(offset_ms, unit='ms')
source_ohlc = ColumnDataSource(df_ohlc)
source_vol_profile = ColumnDataSource(df_profile)

plt_candlestick = figure(x_axis_type='datetime', width=1600, height=800)
candles = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='low', line_color='color_smart_money', source=source_ohlc, alpha=0.4)
plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color_smart_money', line_color=None, source=source_ohlc, legend_label='BTCUSDT 4 Hours Candlesticks', alpha=0.4)
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
    "red_white_green", ["#ff0000", "#ffffff", "#00ff00"]
)

# Convertirlo a una lista de 256 hexadecimales para Bokeh
palette_256 = [matplotlib.colors.rgb2hex(cmap(i)) for i in np.linspace(0, 1, 256)]
color_mapper = LinearColorMapper(
        palette=palette_256,
        low=-1,
        high=1
    )
heatmap = plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution*0.9, line_color='black', line_alpha=0.3, color={'field': 'delta_normalized', 'transform': color_mapper}, source=source_vol_profile, alpha=0.4)
hover_heatmap = HoverTool(
    renderers=[heatmap],
    tooltips=[
        ("Tiempo", "@open_time{%F %T}"),
        ("Precio", "@price_bin{0,0.00}"),
        ("Volumen", "@total_volume{0,0.00}"),
        ("Buy Volume", "@buy_volume{0,0.00}"),
        ("Sell Volume", "@sell_volume{0,0.00}"),
        ("Delta", "@delta{+0,0.00}"),
        ("Delta Normalized", "@delta_normalized{0.00}")
    ], formatters={'@open_time': 'datetime'})
plt_candlestick.add_tools(hover_heatmap)
show(plt_candlestick)