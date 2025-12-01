from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter
from bokeh.layouts import column
import pandas as pd
import matplotlib
import numpy as np
from agg_trades_db import AggTradeDB
from bokeh.models import LinearColorMapper
from bokeh.palettes import RdYlGn11, linear_palette
import os

interval = '4 hours'
interval_trades = '10 minutes'
size_position = 1
resolution = 100
start = '2025-10-01'
end = '2025-11-23'
simbol = 'BTCUSDT'
db_path = f'data/{simbol}/tradebook/agg_trades.db'
table = 'agg_trades'
path = 'bokeh_output'
os.makedirs(path, exist_ok=True)
file_path = os.path.join(path, 'volume_profile.html')
output_file(file_path)
# Consultas a la base de datos
db_connector = AggTradeDB(db_path)
df_ohlc = db_connector.get_ohlc(interval, start, end)
df_vol_profile = db_connector.get_volume_profile(interval, start, end, resolution)
df_profile = db_connector.get_profile(start, end, resolution)
df_trades = db_connector.get_institutional_trades(start, end, interval_trades)
db_connector.close_connection()
# Calculos de variables
width_ms = (df_ohlc['open_time'].iloc[1] - df_ohlc['open_time'].iloc[0]).total_seconds() * 1000
offset_ms = width_ms * 0.5
volume_normalized = 'volume_global_normalized'
delta_normalized = 'delta_global_normalized'
# Crear un colormap personalizado
cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
    "red_white_green", ["#b50000", "#ffffff", "#007000"]
)

# Convertirlo a una lista de 256 hexadecimales para Bokeh
palette_256 = [matplotlib.colors.rgb2hex(cmap(i)) for i in np.linspace(0, 1, 256)]
# Configurar maximos y minimos para el LinearColorMapper
delta_min = df_vol_profile[delta_normalized].min()
delta_max = df_vol_profile[delta_normalized].max()
color_mapper = LinearColorMapper(
        palette=palette_256,
        low=delta_min,
        high=delta_max
    )
df_vol_profile['bar_left'] = df_vol_profile['open_time'] - pd.to_timedelta(offset_ms, unit='ms')
df_vol_profile['bar_right'] = df_vol_profile['bar_left'] + pd.to_timedelta(df_vol_profile[volume_normalized] * width_ms, unit='ms')

source_ohlc = ColumnDataSource(df_ohlc)
source_vol_profile = ColumnDataSource(df_vol_profile)
# Grafica Cuerpo y mecha de la vela
plt_candlestick = figure(x_axis_type='datetime', width=1600, height=600)
candles = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='low', line_color='color', source=source_ohlc, alpha=0.2)
plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color', line_color=None, source=source_ohlc, legend_label=f'{simbol}-{interval} Candlesticks', alpha=0.2)
# Grafica perfil de volumen por precio y tiempo
heatmap = plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution*0.9, line_color='black', line_alpha=0.3, color={'field': delta_normalized, 'transform': color_mapper}, source=source_vol_profile, alpha=0.4)
# Grafica POC de cada vela
df_poc = df_vol_profile.loc[df_vol_profile.groupby('open_time')['total_volume'].idxmax()][['open_time', 'price_bin', 'bar_left', 'bar_right', 'total_volume']].reset_index(drop=True).sort_values('open_time')
plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution, color=None, line_color='black', source=ColumnDataSource(df_poc))
# Grafica trades institucionales
df_filtered = df_trades[df_trades['quantity'] >= size_position][['quantity', 'is_buyer_maker', 'trade_count', 'interval_time', 'avg_price']]
df_filtered['color'] = df_filtered['is_buyer_maker'].map({
    True: 'red',    # Seller taker
    False: 'green'  # Buyer taker
})
df_filtered['quantity_scaled'] = df_filtered['quantity'] * 4
source_trades = ColumnDataSource(df_filtered)
plt_candlestick.scatter(x='interval_time', y='avg_price', size='quantity_scaled', fill_color='color', line_color=None, line_width=2, source=source_trades, legend_label='Institucional Sell Trades', alpha=0.6)
# Hover para las mechas de las velas
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
# Hover para el perfil de volumen
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
plt_candlestick.add_tools(hover_heatmap)
# HOver para trades institucionales
hover_trades = HoverTool(
    tooltips=[
        ("Quantity", "@quantity{0.00}"),
        ("Trade Count", "@trade_count{0}"),
        ("Interval Time", "@interval_time{%F %T}"),
        ("Avg Price", "@avg_price{0,0.00}")
    ], formatters={'@interval_time': 'datetime'})
plt_candlestick.add_tools(hover_trades)
# Graficar volumen 
window=20
plt_volume = figure(x_axis_type='datetime', width=1600, height=200, x_range=plt_candlestick.x_range)
df_ohlc['volume_ma'] = df_ohlc[ 'volume'].rolling(window=window).mean()
df_volume = df_ohlc.loc[df_ohlc['volume'] > df_ohlc['volume_ma'] * 1.5, ['open_time', 'volume']]
source_volume = ColumnDataSource(df_volume)
plt_volume.vbar_stack(stackers=['buy_volume', 'sell_volume'], x='open_time', width=width_ms, color=['red', 'green'], source=source_ohlc)
plt_volume.vbar(x='open_time', width=width_ms, top='volume', bottom=0, fill_color=None, line_color='black', source=source_volume)
# Graficar perfil de volumen
start_time = pd.to_datetime(end) + pd.to_timedelta(width_ms, unit='ms')
df_profile['total_volume_normalized'] = start_time - pd.to_timedelta(df_profile['total_volume_normalized'] * width_ms * 4 , unit="ms")
source_profile = ColumnDataSource(df_profile)
plt_candlestick.hbar(y='price_bin', left='total_volume_normalized', right=start_time, height= resolution , fill_color='blue', source=source_profile)
df_filtered['total'] = df_filtered['quantity'] * df_filtered['trade_count'] * df_filtered['avg_price']
df_res = df_filtered.groupby(['quantity', 'is_buyer_maker']).agg(total_USD=('total', 'sum'),
                                                                 ocurrencias=('trade_count', 'count')
                                                                 ).reset_index().sort_values('ocurrencias', ascending=False)
print(df_res.head(20))
show(column(plt_candlestick, plt_volume))