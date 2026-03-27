# GEMINI.md - Backtesting System (BTCUSDT)

Este proyecto es un sistema de análisis y backtesting para criptomonedas (específicamente BTCUSDT) que utiliza datos de trades agregados (`agg_trades`) de Binance. El sistema descarga datos masivos, los procesa en una base de datos DuckDB y genera visualizaciones interactivas con métricas avanzadas de Order Flow y Market Profile.

## 🚀 Resumen del Proyecto
- **Tecnologías:** Python 3.11, DuckDB, Pandas, Bokeh (visualización), Pipenv.
- **Datos:** Binance `agg_trades` (tick-by-tick aggregated data).
- **Arquitectura:** Ingestión de datos -> Procesamiento SQL (DuckDB) -> Análisis y Visualización.

## 🛠️ Instalación y Configuración
El proyecto utiliza `pipenv` para gestionar el entorno virtual y las dependencias.

```bash
# Instalar dependencias
pipenv install

# Entrar al entorno virtual
pipenv shell
```

## 📋 Comandos Clave
- **Poblar Base de Datos:** Descarga datos de Binance y genera las tablas OHLC, Volume Profile y Market Context.
  ```bash
  pipenv run python populate_DB.py
  ```
- **Generar Visualización:** Crea un reporte interactivo en HTML (`bokeh_output/volume_profile.html`).
  ```bash
  pipenv run python graph_candlestick.py
  ```

## 🏗️ Estructura de Datos (DuckDB)
La base de datos se encuentra en `data/BTCUSDT/tradebook/agg_trades.db`.

### Tablas Principales:
1.  **`agg_trades`:** Datos crudos de Binance (price, quantity, timestamp, buyer_maker).
2.  **`ohlc_1_minutes`:** Tabla base procesada con:
    - Métricas estándar: Open, High, Low, Close, Volume.
    - Order Flow: Delta (Buy - Sell Vol), Trade Count.
    - Algoritmos: VWAP, VWAP Std, POC (Point of Control), VWAP Slope.
3.  **`volume_profile`:** Distribución de volumen por niveles de precio (`price_bin`) agrupados por cada vela (`open_time`). 
    - Permite definir la **temporalidad** (ej. 1 min, 15 min) y la **resolución** (ej. bins de $10 o $50) para el análisis de Order Flow detallado.
4.  **`market_context_15_minutes`:** Métricas de régimen de mercado:
    - Efficiency Ratio (Kaufman), R-Squared, Coeficiente de Variación, ATR Normalizado.

## 📝 Convenciones de Desarrollo
- **Encoding en Windows:** Al imprimir símbolos unicode (✓, ✗, ⚠️), usa alternativas ASCII como `[OK]`, `[ERROR]`, `[WARN]` para evitar errores de `cp1252`.
- **DuckDB SQL:**
  - Los timestamps en `agg_trades` están en microsegundos. Conversión: `to_timestamp(timestamp / 1000000)`.
  - Para remuestreo (resampling) utiliza `time_bucket()`.
  - En joins complejos, siempre usa alias de tabla para evitar referencias ambiguas en `open_time`.
- **Visualización:** El output principal es un archivo HTML generado por Bokeh en la carpeta `bokeh_output/`.

## 📂 Archivos Principales
- `agg_trades_to_db.py`: Contiene la clase `AggTradeDB` con toda la lógica de DuckDB.
- `download_agg_trades_binance.py`: Maneja las descargas desde `data.binance.vision`.
- `populate_DB.py`: Script de orquestación para actualización de datos.
- `graph_candlestick.py`: Generador de gráficas de velas y perfiles de volumen.
- `candlestick_analytics.py`: Detección de patrones (ej. Hammer candles).
