import os
import pandas as pd
import numpy as np
from bokeh.plotting import output_file, figure, show
from bokeh.models import ColumnDataSource, WheelZoomTool, HoverTool
from bokeh.layouts import column

class StrategyChart:
    """
    A class to create candlestick charts for different trading strategies.
    """
    def __init__(self, strategy_name: str, signals: pd.DataFrame, metrics: pd.DataFrame):
        self.strategy_name = strategy_name
        self.signals = signals.copy()
        self.signals['timestamp'] = self.signals['timestamp'].dt.tz_localize(None)
        self.signals['exit_time'] = self.signals['exit_time'].dt.tz_localize(None)
        #print(self.signals.columns)
        self.metrics = metrics.copy()
        self.metrics['open_time'] = self.metrics['open_time'].dt.tz_localize(None)
        #print(self.metrics.columns)
        path = 'bokeh_output'
        os.makedirs(path, exist_ok=True)
        output_file(os.path.join(path, f'{self.strategy_name}_chart.html'))
    
    def create_chart(self):    
        
        # Calculos de amcho de vela y offset
        width_ms = (self.metrics['open_time'].iloc[1] - self.metrics['open_time'].iloc[0]).total_seconds() * 1000
        offset = width_ms * 0.5
        
        # mapea color de fondo
        color_event = {
            'BOS_BULL': '#4CAF50',
            'CHOCH_BULL': "#A1F1A1",
            'BOS_BEAR': '#F44336',
            'CHOCH_BEAR': "#F1A1A1"
        }
        
        # Crear gráfico de velas
        CHART_WIDTH = 1900
        CHART_HEIGHT = 600
        VOLUME_HEIGHT = 250
        plt_candlestick = figure(x_axis_type='datetime', height=CHART_HEIGHT, width=CHART_WIDTH)
        # Crear herramientas de zoom separadas
        wheel_zoom_x = WheelZoomTool(dimensions='width')  # Solo zoom horizontal
        wheel_zoom_y = WheelZoomTool(dimensions='height')  # Solo zoom vertical

        # Agregar las herramientas
        plt_candlestick.add_tools(wheel_zoom_x, wheel_zoom_y)

        # Establecer una como activa por defecto
        plt_candlestick.toolbar.active_scroll = wheel_zoom_x
        # Grafica Cuerpo y mecha de la vela
        ohlc = self.metrics[["open_time", "open", "high", "low", "close", 'color', 'poc', 'delta_normalized', 'volume', 'volume_ma', 'trade_count']].copy()
        # Calcula las mechas basándote en el color
        ohlc['upper_wick_end'] = np.where(
            ohlc['color'] == 'red', 
            ohlc['open'], 
            ohlc['close']
        )
        ohlc['lower_wick_end'] = np.where(
            ohlc['color'] == 'red', 
            ohlc['close'], 
            ohlc['open']
        )
        source_ohlc = ColumnDataSource(ohlc)
        upper_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='upper_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
        lower_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='low', y1='lower_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
        body = plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color', line_color='color',  line_width=2, source=source_ohlc, alpha=0.2)
        poc = plt_candlestick.rect(x='open_time', y='poc', width=width_ms*0.8, height=10, fill_color='black', line_color='black', source=source_ohlc, alpha=0.8)
        
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
                ("Volume MA", "@volume_ma{0.00}"),
                ("Delta Normalized", "@delta_normalized{0.2f}"),
                ("Trade Count", "@trade_count"),
                ("VWAP", "@vwap{0.2f}"),
                ("POC", "@poc{0.2f}"),
                ("Price/VWAP Diff", "@price_vwap_diff{0.00%}"),
                ("VWAP Slope", "@vwap_slope{0.2f}"),
                ("last_sh_type", "@last_sh_type"),
                ("last_sl_type", "@last_sl_type"),
                ("swing_score", "@swing_score"),
                ("last_event", "@last_event")
            ],
            formatters={
                '@open_time': 'datetime',
            },
            mode='vline'
        )
        plt_candlestick.add_tools(hover)
        
        # Grafica fondo con tendencia
        trend = self.metrics[["open_time", "trend"]].copy()
        trend['bg_color'] = trend['trend'].map(color_event).fillna('#888888')
        trend['close_time'] = trend['open_time'] + pd.to_timedelta(width_ms, unit='ms')
        trend['sum'] = trend['trend'].ne(trend['trend'].shift(1)).cumsum()
        trend = trend.groupby('sum').agg({
            'open_time': 'first',
            'close_time': 'last',
            'bg_color': 'first'
        }).reset_index(drop=True)
        
        # Establecer límites dinámicos para el eje y
        y_min = self.metrics['low'].min() * 0.98
        y_max = self.metrics['high'].max() * 1.02
        
        source_trend = ColumnDataSource(trend)
        bg_trend = plt_candlestick.quad(
            left='open_time',
            right='close_time',
            bottom=y_min,
            top=y_max,
            fill_color='bg_color',
            line_color=None,
            fill_alpha=0.4,
            source=source_trend,
            level='underlay'
        )
        
        #Graficar señales de entrada y salida
        signals = self.signals[["timestamp", "entry_trigger", "exit_time", "exit_price"]]
        plt_candlestick.scatter(x='timestamp', y='entry_trigger', size=10, color='green', source=ColumnDataSource(signals), legend_label='Entry')
        plt_candlestick.scatter(x='exit_time', y='exit_price', size=10, color='red', source=ColumnDataSource(signals), legend_label='Exit')
        show(column(plt_candlestick,))
        