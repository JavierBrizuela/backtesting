import os
import pandas as pd
from bokeh.plotting import output_file, figure
from bokeh.models import ColumnDataSource

class StrategyChart:
    """
    A class to create candlestick charts for different trading strategies.
    """
    def __init__(self, strategy_name: str, signals: pd.DataFrame, metrics: pd.DataFrame):
        self.strategy_name = strategy_name
        self.signals = signals
        self.metrics = metrics
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
        
        # Establecer límites dinámicos para el eje y
        y_min = self.metrics['low'].min() * 0.98
        y_max = self.metrics['high'].max() * 1.02
        
        # Crear gráfico de velas
        CHART_WIDTH = 1900
        CHART_HEIGHT = 600
        VOLUME_HEIGHT = 250
        plt_candlestick = figure(x_axis_type='datetime', height=CHART_HEIGHT, width=CHART_WIDTH)
        # Grafica fondo de eventos
        trend = self.metrics[["open_time", "trend"]].copy()
        trend['bg_color'] = trend['trend'].map(color_event).fillna('#888888')
        trend['close_time'] = trend['open_time'] + pd.Timedelta(milliseconds=width_ms)
        source_trend = ColumnDataSource(trend)
        bg_trends = plt_candlestick.quad(
            left='open_time',
            right='close_time',
            bottom=y_min,
            top=y_max,
            fill_color='bg_color',
            line_color=None,
            alpha=0.4,
            source=source_trend,
            level='underlay'
        )
        # Grafica Cuerpo y mecha de la vela
        upper_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='high', y1='upper_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
        lower_wick = plt_candlestick.segment(x0='open_time', x1='open_time', y0='low', y1='lower_wick_end', line_color='color', line_width=2, source=source_ohlc, alpha=0.2)
        body = plt_candlestick.vbar(x='open_time', width=width_ms, top='open', bottom='close', fill_color='color', line_color='color',  line_width=2, source=source_ohlc, legend_label=f'{simbol}-{interval} Candlesticks - {resolution} resolution', alpha=0.2)