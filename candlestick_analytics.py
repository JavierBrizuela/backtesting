def candlestick_proportions(open, high, low, close):
    """Calcula las proporciones de las mechas y el cuerpo de una vela."""
    body_size = abs(close - open)
    upper_wick_size = high - max(open, close)
    lower_wick_size = min(open, close) - low
    total_size = body_size + upper_wick_size + lower_wick_size

    if total_size == 0:
        return 0, 0, 0  # Evitar división por cero

    body_proportion = body_size / total_size
    upper_wick_proportion = upper_wick_size / total_size
    lower_wick_proportion = lower_wick_size / total_size

    return body_proportion, upper_wick_proportion, lower_wick_proportion

def hammer_candle(open, high, low, close, body_thresh=0.3, lower_wick_thresh=0.5):
    """Determina si una vela es un martillo según las proporciones de sus partes."""
    body, upper_wick, lower_wick = candlestick_proportions(open, high, low, close)
    return body <= body_thresh and lower_wick >= lower_wick_thresh