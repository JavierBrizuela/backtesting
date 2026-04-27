from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter, WheelZoomTool, CrosshairTool, Span
from bokeh.layouts import column
import pandas as pd
import matplotlib
import numpy as np
from analytics_db import AnalyticsDB
from bokeh.models import LinearColorMapper
from bokeh.palettes import RdYlGn11, linear_palette
import os
from candlestick_analytics import hammer_candle

# Parámetros de configuración simbolo, intervalo, fechas, base de datos
interval = '4 hours'
start = '2026-01-01'
end = '2026-03-24'
simbol = 'BTCUSDT'
raw_path = f'data/{simbol}/tradebook/raw_data.db'
analytics_path = f'data/{simbol}/tradebook/analytics.db'
table = 'agg_trades'
window=20

# Parámetros de análisis de trades institucionales
interval_trades = '5 minutes'
size_position = 1

# Parámetros de perfil de volumen
resolution = 10
volume_normalized = 'volume_local_normalized' # 'volume_global_normalized'
delta_normalized = 'delta_local_normalized' # 'delta_global_normalized'

# Parámetros de la gráfica
path = 'bokeh_output'
os.makedirs(path, exist_ok=True)
file_path = os.path.join(path, 'volume_profile.html')
output_file(file_path)
CHART_WIDTH = 1900
CHART_HEIGHT = 600
VOLUME_HEIGHT = 250


# Consultas a la base de datos
db_connector = AnalyticsDB(analytics_path)
df_ohlc = db_connector.get_ohlc(interval, start, end)
df_vol_profile = db_connector.get_volume_profile(interval, start, end, resolution)
df_profile = db_connector.get_profile(start, end, resolution)
# df_trades = db_connector.get_institutional_trades(start, end, interval_trades)
df_market_context = db_connector.get_market_context(interval, start, end)
db_connector.close_connection()

# Convertir ambos dataframes y quitar el tzinfo
df_ohlc['open_time'] = df_ohlc['open_time'].dt.tz_localize(None)
df_vol_profile['open_time'] = df_vol_profile['open_time'].dt.tz_localize(None)
df_market_context['open_time'] = df_market_context['open_time'].dt.tz_localize(None)

# Calculos de amcho de vela y offset
width_ms = (df_ohlc['open_time'].iloc[1] - df_ohlc['open_time'].iloc[0]).total_seconds() * 1000
offset_ms = width_ms * 0.5
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
source_vol_profile = ColumnDataSource(df_vol_profile)

# Mapea el color del fondo
color_trend = {
    'BULLISH': '#4CAF50',
    'TRANSITIONING_BULL': "#A1F1A1",
    'BEARISH': '#F44336',
    'TRANSITIONING_BEAR': "#F1A1A1",
    'RANGING': "#2767B0"
}

# Calcula las mechas basándote en el color
df_ohlc['upper_wick_end'] = np.where(
    df_ohlc['color'] == 'red', 
    df_ohlc['open'], 
    df_ohlc['close']
)

df_ohlc['lower_wick_end'] = np.where(
    df_ohlc['color'] == 'red', 
    df_ohlc['close'], 
    df_ohlc['open']
)
# Calcula los bloques de tendencia y asigna el color de fondo correspondiente
trend = df_market_context[["open_time", "trend_refined"]].copy()
trend['bg_color'] = trend['trend_refined'].map(color_trend)
trend['close_time'] = trend['open_time'].shift(-1)
source_trend = ColumnDataSource(trend)
source_ohlc = ColumnDataSource(df_ohlc) 
plt_candlestick = figure(x_axis_type='datetime', height=CHART_HEIGHT, width=CHART_WIDTH)
y_min = df_ohlc['low'].min() * 0.98
y_max = df_ohlc['high'].max() * 1.02
bg_rects = plt_candlestick.quad(
    left='open_time',
    right='close_time',
    bottom=y_min,        # o un valor absurdamente bajo
    top=y_max,   # o absurdamente alto
    fill_color='bg_color',
    line_color=None,
    alpha=0.08,
    source=source_trend,
    level='underlay'
)

# Muestra HH LH HL LL
swing_highs = df_market_context[df_market_context['is_sh']][['open_time', 'high', 'sh_type']].copy()
swing_highs['alpha'] = np.where(swing_highs['sh_type'] == 'HH', 1, 0.5)
swing_lows = df_market_context[df_market_context['is_sl']][['open_time', 'low', 'sl_type']].copy()
swing_lows['alpha'] = np.where(swing_lows['sl_type'] == 'LL', 1, 0.5)
source_swing_highs = ColumnDataSource(swing_highs)
source_swing_lows = ColumnDataSource(swing_lows)
plt_candlestick.scatter(x='open_time', y='high', size=10, fill_color='green', line_color=None, alpha='alpha', source=source_swing_highs, legend_label='Swing Highs')
plt_candlestick.scatter(x='open_time', y='low', size=10, fill_color='red', line_color=None, alpha='alpha', source=source_swing_lows, legend_label='Swing Lows')

# Grafica Cuerpo y mecha de la vela
upper_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='upper_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
lower_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='low', y1='lower_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
body = plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color', line_color='color',  line_width=2, source=source_ohlc, legend_label=f'{simbol}-{interval} Candlesticks - {resolution} resolution', alpha=0.2)

# Grafica perfil de volumen por precio y tiempo
#heatmap = plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution*0.9, line_color='black', line_alpha=0.3, color={'field': delta_normalized, 'transform': color_mapper}, source=source_vol_profile, alpha=0.4)
# Grafica POC de cada vela
df_poc = df_vol_profile.loc[df_vol_profile.node_type.values == 'POC'][['open_time', 'price_bin', 'bar_left', 'bar_right', 'total_volume']].reset_index(drop=True).sort_values('open_time')
plt_candlestick.hbar(y='price_bin', left='bar_left', right='bar_right', height=resolution, color=None, line_color='black', source=ColumnDataSource(df_poc))

# Grafica trades institucionales
""" df_filtered = df_trades[df_trades['quantity'] == size_position][['quantity', 'is_buyer_maker', 'trade_count', 'interval_time', 'avg_price']]
df_filtered['color'] = df_filtered['is_buyer_maker'].map({
    True: 'red',    # Seller taker
    False: 'green'  # Buyer taker
})
df_filtered['quantity_scaled'] = df_filtered['quantity'] * 4
source_trades = ColumnDataSource(df_filtered)
plt_candlestick.scatter(x='interval_time', y='avg_price', size='quantity_scaled', fill_color='color', line_color=None, line_width=2, source=source_trades, legend_label='Institucional Sell Trades', alpha=0.6)
 """
# Hover para las velas
hover = HoverTool(
    renderers=[body],
    tooltips=[
        ("Time", "@open_time{%F %T}"),
        ("Open", "@open{0.2f}"),
        ("High", "@high{0.2f}"),
        ("Low", "@low{0.2f}"),
        ("Close", "@close{0.2f}"),
        ("Volume", "@volume{0.00}"),
        ("Trade Count", "@trade_count"),
        ("VWAP", "@vwap{0.2f}"),
        ("POC", "@poc{0.2f}"),
        ("Price/VWAP Diff", "@price_vwap_diff{0.00%}"),
        ("VWAP Slope", "@vwap_slope{0.2f}"),
    ],
    formatters={
        '@open_time': 'datetime',
    },
    mode='vline'
)
# Crear herramientas de zoom separadas
wheel_zoom_x = WheelZoomTool(dimensions='width')  # Solo zoom horizontal
wheel_zoom_y = WheelZoomTool(dimensions='height')  # Solo zoom vertical

# Agregar las herramientas
plt_candlestick.add_tools(wheel_zoom_x, wheel_zoom_y)

# Establecer una como activa por defecto
plt_candlestick.toolbar.active_scroll = wheel_zoom_x
plt_candlestick.add_tools(hover)
plt_candlestick.xaxis.visible = False 
plt_candlestick.yaxis.formatter = NumeralTickFormatter(format="0,0.00")
# Hover para el perfil de volumen por precio y tiempo
""" hover_heatmap = HoverTool(
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
plt_candlestick.add_tools(hover_heatmap) """

# Hover para trades institucionales
hover_trades = HoverTool(
    tooltips=[
        ("Quantity", "@quantity{0.00}"),
        ("Trade Count", "@trade_count{0}"),
        ("Interval Time", "@interval_time{%F %T}"),
        ("Avg Price", "@avg_price{0,0.00}")
    ], formatters={'@interval_time': 'datetime'})
#plt_candlestick.add_tools(hover_trades)

# Graficar volumen compra ventas apilados
plt_volume = figure(x_axis_type='datetime', height=VOLUME_HEIGHT, width=CHART_WIDTH, x_range=plt_candlestick.x_range)
df_ohlc['volume_ma'] = df_ohlc[ 'volume'].rolling(window=window).mean()
df_ohlc['volume_high'] = df_ohlc['volume'] > df_ohlc['volume_ma'] * 1.8
df_ohlc['delta_cum'] = df_ohlc['delta'].cumsum()
df_ohlc['delta_ma'] = df_ohlc['delta'].rolling(window=window).sum()
df_ohlc['delta_normalized'] = df_ohlc['buy_volume'] / df_ohlc['volume']
df_ohlc['delta_normalized_scaled'] = (df_ohlc['delta_normalized']-0.5) * df_ohlc['volume'].max()
source_volume = ColumnDataSource(df_ohlc)
plt_volume.vbar(x='open_time', width=width_ms, top='volume', bottom=0, fill_color=None, line_color='black', line_width=2, source=ColumnDataSource(df_ohlc[df_ohlc['volume_high']]), legend_label='Total Volume')
plt_volume.vbar_stack(stackers=['sell_volume', 'buy_volume'], x='open_time', width=width_ms * 0.9, color=['red', 'green'], line_color=None, source=source_volume)
plt_volume.line(x='open_time', y='volume_ma', line_color='blue', line_width=2, legend_label=f'Volume MA {window}', source=source_volume)
plt_volume.line(x='open_time', y='delta_ma', line_color='orange', line_width=2, legend_label=f'Delta MA {window}', source=source_volume)
plt_volume.line(x='open_time', y='delta_normalized_scaled', line_color='purple', line_width=2, legend_label='Delta Normalized', source=source_volume)
# Hover para volumen
hover_volume = HoverTool(
    tooltips=[
        ("Time", "@open_time{%F %T}"),
        ("Buy Volume", "@buy_volume{0,0.00}"),
        ("Sell Volume", "@sell_volume{0,0.00}"),
        ("Total Volume", "@volume{0,0.00}"),
        ("Delta", "@delta{+0,0.00}"),
        ("Volume MA", "@volume_ma{0,0.00}"),
        ("Delta MA", "@delta_ma{+0,0.00}"),
        ("Delta Normalized", "@delta_normalized{0.00}"),
        ("Overlap Ratio", "@overlap_ratio{0.00}")
    ], formatters={'@open_time': 'datetime'})
plt_volume.add_tools(hover_volume)
plt_volume.yaxis.formatter = NumeralTickFormatter(format="0,0.00")

# Grafica de contexto de mercado
""" df_market_context['efficiency_normalized'] = df_market_context['efficiency'] / df_market_context['efficiency'].max()
df_market_context['efficiency_ma'] = df_market_context['efficiency_normalized'].rolling(window=window).mean()
df_market_context['delta_efficiency_normalized'] = df_market_context['delta_efficiency'] / max(df_market_context['delta_efficiency'].abs().max(), df_market_context['delta_efficiency'].abs().min())
plt_context = figure(x_axis_type='datetime', height=250, width=CHART_WIDTH, x_range=plt_candlestick.x_range, title='Market Context')
source_market_context = ColumnDataSource(df_market_context)
hline_hight = Span(location=0.6 , dimension='width', line_color='black', line_width=2, line_dash='dashed')
hline_low = Span(location=0.3 , dimension='width', line_color='black', line_width=2, line_dash='dashed')
hline_r_square = Span(location=0.7 , dimension='width', line_color='blue', line_width=2, line_dash='dashed')
plt_context.add_layout(hline_hight)
plt_context.add_layout(hline_low)
plt_context.add_layout(hline_r_square)
plt_context.line(x='open_time', y='efficiency_normalized', line_color='red', line_width=2, source=source_market_context, legend_label='Efficiency', alpha=0.5)
plt_context.line(x='open_time', y='efficiency_ma', line_color='red', line_width=2, source=source_market_context, legend_label=f'Efficiency MA {window}')
plt_context.line(x='open_time', y='efficiency_ratio', line_color='black', line_width=2, source=source_market_context, legend_label='efficiency ratio Kaufman')
plt_context.line(x='open_time', y='r_squared', line_color='blue', line_width=1, source=source_market_context, legend_label='R²')
plt_context.line(x='open_time', y='delta_efficiency_normalized', line_color='green', line_width=2, source=source_market_context, legend_label='delta efficiency normalized', alpha=0.7) """
#plt_context.line(x='open_time', y='coefficient_variation', line_color='orange', line_width=1, source=source_market_context, legend_label='Coefficient of Variation')
#plt_context.line(x='open_time', y='atr_normalized', line_color='purple', line_width=1, source=source_market_context, legend_label='ATR Normalized')

#Hover para contexto de mercado
hover_context = HoverTool(
    tooltips=[
        ("Time", "@open_time{%F %T}"),
        ("Efficiency", "@efficiency_normalized{0.00}"),
        ("Efficiency MA", "@efficiency_ma{0.00}"),
        ("Efficiency Ratio Kaufman", "@efficiency_ratio{0.00}"),
        ("R²", "@r_squared{0.00}"),
        ("Coefficient of Variation", "@coefficient_variation{0.00}"),
        ("ATR Normalized", "@atr_normalized{0.00}"),
        ("Delta Efficiency", "@delta_efficiency{+0,0.00}")
    ], formatters={'@open_time': 'datetime'})
#plt_context.add_tools(hover_context)
# Grafica velas que cumplas los requisitos
df_ohlc['hammer_candle'] = df_ohlc.apply(lambda row: hammer_candle(row['open'], row['high'], row['low'], row['close']), axis=1)
# POC ya viene de la DB en df_ohlc['poc'], no hace falta calcularlo

""" df_ohlc = df_ohlc.merge(df_market_context[['open_time', 'efficiency_normalized', 'efficiency_ma']], on='open_time', how='left')
df_buyl = df_ohlc[(df_ohlc['poc'] <= df_ohlc['close'] + abs(df_ohlc['close'] - df_ohlc['open']) * 0.2) & 
                 (df_ohlc['volume_high'] == True) & 
                 (df_ohlc['delta_normalized'] < 0.46) & 
                 (df_ohlc['color'] == 'red') &
                 (df_ohlc['efficiency_normalized'] < df_ohlc['efficiency_ma'])]
print(df_buyl.head())
plt_candlestick.vbar(x='open_time', width=width_ms, top='high', bottom='low', fill_color=None, line_color='green', line_width=2, fill_alpha=0.4, source=ColumnDataSource(df_buyl), legend_label='absorption', alpha=0.8)
df_sell = df_ohlc[(df_ohlc['poc'] >= df_ohlc['close'] ) & 
                 (df_ohlc['volume_high'] == True) & 
                 (df_ohlc['delta_normalized'] > 0.5) & 
                 (df_ohlc['color'] == 'green') &
                 (df_ohlc['delta_ma'] > 0)]
plt_candlestick.vbar(x='open_time', width=width_ms, top='high', bottom='low', fill_color=None, line_color='red', line_width=2, fill_alpha=0.4, source=ColumnDataSource(df_sell), legend_label='absorption', alpha=0.8)
 """
# Graficar imbalance oferta y demanda
""" IMB_RATIO = 3.0
MIN_STREAK = 3

def streak_counter(series):
    streaks = []
    current_streak = 0
    for value in series:
        if value:
            current_streak += 1
        else:
            current_streak = 0
        streaks.append(current_streak)
    return streaks

def bin_filled(row, df_ohlc):
    future = df_ohlc[df_ohlc['open_time'] > row['open_time']]
    if row['buy_imbalance']:
        hits = future[future['low'] <= row['price_bin']]
    else:
        hits = future[future['high'] >= row['price_bin']]
    if hits.empty:
        return pd.to_datetime(end)
    else:
        return hits['open_time'].iloc[0]
    
df_imbalance = df_vol_profile.sort_values(['open_time', 'price_bin']).reset_index(drop=True)
df_imbalance['sell_prev_bin'] = df_imbalance.groupby('open_time')['sell_volume'].shift(1)
df_imbalance['imbalance_ratio'] = df_imbalance['buy_volume'] / df_imbalance['sell_prev_bin']
df_imbalance = df_imbalance.merge( df_ohlc[['open_time', 'open', 'high', 'low', 'close']], on='open_time', how='left' )
df_imbalance['buy_imbalance'] = (df_imbalance['imbalance_ratio'] >= IMB_RATIO) & (df_imbalance['price_bin'] < df_imbalance['close']) 
df_imbalance['buy_streak'] = df_imbalance.groupby('open_time')['buy_imbalance'].transform(streak_counter)
df_buy_imbalance = df_imbalance[df_imbalance['buy_streak'] >= MIN_STREAK].copy()
df_buy_imbalance['imbalance_filled'] = df_buy_imbalance.apply(lambda row: bin_filled(row, df_ohlc), axis=1)
plt_candlestick.hbar(y='price_bin', left='open_time', right= 'imbalance_filled', height=resolution*0.9, fill_color='green', line_color=None, source=ColumnDataSource(df_buy_imbalance), alpha=0.8)    
df_imbalance['sell_imbalance'] = (df_imbalance['imbalance_ratio'] <= (1/IMB_RATIO)) & (df_imbalance['price_bin'] > df_imbalance['close'])
df_imbalance['sell_streak'] = df_imbalance.groupby('open_time')['sell_imbalance'].transform(streak_counter)
df_sell_imbalance = df_imbalance[df_imbalance['sell_streak'] >= MIN_STREAK].copy()
df_sell_imbalance['imbalance_filled'] = df_sell_imbalance.apply(lambda row: bin_filled(row, df_ohlc), axis=1)
plt_candlestick.hbar(y='price_bin', left='open_time', right= 'imbalance_filled', height=resolution*0.9, fill_color='red', line_color=None, source=ColumnDataSource(df_sell_imbalance), alpha=0.8)    
 """
# Graficar perfil de volumen
start_time = pd.to_datetime(end) + pd.to_timedelta(width_ms, unit='ms')
df_profile['total_volume_normalized'] = start_time - pd.to_timedelta(df_profile['total_volume_normalized'] * width_ms * 4 , unit="ms")
source_profile = ColumnDataSource(df_profile)
plt_candlestick.hbar(y='price_bin', left='total_volume_normalized', right=start_time, height= resolution , fill_color='blue', source=source_profile, alpha=0.6)

# df_filtered['total'] = df_filtered['quantity'] * df_filtered['trade_count'] * df_filtered['avg_price']
# df_res = df_filtered.groupby(['quantity', 'is_buyer_maker']).agg(total_USD=('total', 'sum'),
#                                                                  ocurrencias=('trade_count', 'count')
#                                                                  ).reset_index().sort_values('ocurrencias', ascending=False)
# 
crosshair = CrosshairTool(
    dimensions='both',
    line_color='gray',
    line_alpha=0.8,
    line_width=2
)
plt_candlestick.add_tools(crosshair)
plt_volume.add_tools(crosshair)
#plt_candlestick.scatter()
print(df_market_context.head(40))
show(column(plt_candlestick, plt_volume)) # , plt_volume, plt_context