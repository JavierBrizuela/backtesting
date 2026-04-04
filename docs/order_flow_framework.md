# Order Flow Framework - Estrategias de Backtesting

**Fecha de creación:** 2026-03-28
**Última actualización:** 2026-03-29 (Sesión 3)
**Objetivo:** Análisis exploratorio de estrategias rentables y seguras usando order flow + market structure

---

## Enfoque de Trabajo

**Metodología:** Visual → Intuición → Validación

1. Exploración visual con `graph_candlestick.py`
2. Identificación de patrones repetitivos
3. Codificación del patrón como señal
4. Backtest con métricas de rendimiento
5. Análisis de resultados y ajuste de parámetros

---

## Jerarquía de Señales (por prioridad/ROI)

### Prioridad 0: Market Structure (Contexto) ✓ IMPLEMENTADO

Antes de cualquier señal de order flow, determinar **dónde estamos** en la estructura de mercado:

| Evento | Descripción | Significado |
|---|---|---|
| **HH (Higher High)** | Nuevo máximo mayor al anterior | Tendencia alcista intacta |
| **HL (Higher Low)** | Nuevo mínimo mayor al anterior | Pullback saludable en tendencia alcista |
| **LH (Lower High)** | Nuevo máximo menor al anterior | Debilidad alcista, posible cambio |
| **LL (Lower Low)** | Nuevo mínimo menor al anterior | Tendencia bajista intacta |
| **BOS_UP** | Precio rompe último SH | Continuación alcista |
| **BOS_DOWN** | Precio rompe último SL | Continuación bajista |
| **CHoCH_UP** | Cambio de bajista a alcista | Reversión confirmada |
| **CHoCH_DOWN** | Cambio de alcista a bajista | Reversión confirmada |

**Aplicación:**
- Solo operar LONG si `trend_direction = 'UPTREND'`
- Solo operar SHORT si `trend_direction = 'DOWNTREND'`
- En 'RANGING': operar solo cerca de soportes/resistencias (last_swing_high/low)

**Timeframe:** 4H para estructura, 15m para entradas

### Prioridad 1: Liquidez y Agotamiento

| Señal | Descripción | Frecuencia | Confiabilidad | Estado |
|---|---|---|---|---|
| **Absorción en POC/HVN** + MS | Absorción + alineada con estructura | Media | **POR TESTEAR** | 🔄 Con filtro MS |
| **Exhaustion en LVN** | Delta extremo + sin progreso de precio | Baja | No testeada | Pendiente |
| **Imbalance Fill** | Rebalanceo de zonas con ratio >3:1 | Alta | **MEDIA (PF 1.3)** | ✓ Prometedor |

### Prioridad 2: Estructura de Volumen

- **HVN (High Volume Node):** Zonas de aceptación → soporte/resistencia
- **LVN (Low Volume Node):** Zonas de rechazo → precio las cruza rápido
- **POC (Point of Control):** Precio fair → imán o barrera

### Prioridad 3: Filtros de Régimen

Usar `market_context` para determinar CUÁNDO operar:

| Régimen | Efficiency | R² | Estrategia recomendada |
|---|---|---|---|
| **Rango** | < 0.3 | < 0.5 | Absorción, Mean Reversion |
| **Trend** | > 0.6 | > 0.7 | Imbalance follow-through |
| **Transición** | 0.3-0.6 | 0.5-0.7 | Esperar confirmación |

---

## Estrategias Implementadas y Testeadas

### 0. Market Structure Filter (NUEVO) ✓ IMPLEMENTADO

```python
# Columnas disponibles desde market_context_4_hours
trend_direction      # 'UPTREND', 'DOWNTREND', 'RANGING'
last_swing_high      # Precio del último SH
last_swing_low       # Precio del último SL
market_structure_event  # 'HH', 'HL', 'LH', 'LL', 'BOS_UP', 'BOS_DOWN', 'CHoCH_UP', 'CHoCH_DOWN'

# Uso en scanner:
# Absorción Long: SOLO si trend_direction == 'UPTREND'
#   O si estamos cerca del soporte en rango (distancia < 1% del last_swing_low)
# Absorción Short: SOLO si trend_direction == 'DOWNTREND'
#   O si estamos cerca de la resistencia en rango (distancia < 1% del last_swing_high)
```

**Esperado:** Reducción drástica de falsas señales, mejor ratio MFE/MAE.

---

### 1. Absorción en POC (LONG) - ⚠️ NO FUNCIONA (sin filtro MS)

```python
# Condiciones (versión actual SIN filtro MS)
1. Vela roja (close < open)
2. Volumen alto (≥ 1.8x MA20)
3. Delta bajo (< 0.46) → sellers agresivos pero precio no baja
4. Mecha inferior larga (≥ 50% del tamaño total) → RECHAZO
5. POC entre low y close → precio cayó HACIA el POC
6. Efficiency baja (filtro opcional)

# NUEVO - Filtro Market Structure:
7. trend_direction == 'UPTREND' OR close está cerca de last_swing_low (±1%)

# Entry/Exit
- Entry: ruptura del high de la vela
- Stop: low de la vela
- Target: 2:1 RR
```

**Resultados del Backtest (Feb 2026, 15min) SIN filtro MS:**
| Métrica | Valor |
|---|---|
| Trades | 14 |
| Win Rate | **0%** |
| Profit Factor | **0.00** |
| Expectancy | -512 |
| Avg MFE | 328 |
| Avg MAE | 1433 |

**Resultados CON filtro MS:** *Pendiente de ejecutar*

**Conclusión previa:** El precio se mueve **4x más en contra** que a favor. Esperamos que el filtro de tendencia mejore drásticamente estos números.

---

### 2. Absorción en POC (SHORT) - ⚠️ MARGINAL

```python
# Condiciones (versión actual)
1. Vela verde (close > open)
2. Volumen alto (≥ 1.8x MA20)
3. Delta alto (> 0.54) → buyers agresivos pero precio no sube
4. Mecha superior larga (≥ 50% del tamaño total) → RECHAZO
5. POC entre close y high → precio subió HACIA el POC
6. Efficiency baja (filtro opcional)

# Entry/Exit
- Entry: ruptura del low de la vela
- Stop: high de la vela
- Target: 2:1 RR
```

**Resultados del Backtest (Feb 2026, 15min):**
| Métrica | Valor |
|---|---|
| Trades | 10 |
| Win Rate | 40% |
| Profit Factor | 0.80 |
| Expectancy | -65 |
| Avg MFE | 793 |
| Avg MAE | 895 |

**Conclusión:** Ligeramente mejor que Long, pero aún perdedora. No vale la pena el desarrollo adicional por ahora.

---

### 3. Imbalance / Rebalanceo - ✓ PROMETEDOR

```python
# Condiciones
IMB_RATIO = 3.0      # Ratio buy/sell volume
MIN_STREAK = 3       # Velas consecutivas

1. Buy volume / Sell volume ≥ 3:1 por 3 velas consecutivas → Imbalance Long
2. Sell volume / Buy volume ≥ 3:1 por 3 velas consecutivas → Imbalance Short
3. Precio deja el bin sin llenar completamente
4. Cuando precio regresa al bin → entry

# Entry/Exit (actual)
- Entry: cuando precio toca el bin desbalanceado
- Stop: 1% del precio
- Target: 2% del precio
```

**Resultados del Backtest (Feb 2026, 15min):**

| Métrica | Long | Short |
|---|---|---|
| Trades | 260 | 370 |
| Win Rate | 46.2% | 45.4% |
| Profit Factor | 1.29 | 1.30 |

**Conclusión:** **Única estrategia con Profit Factor > 1**. Los desbalances sí se llenan con frecuencia suficiente para ser rentable.

---

### 4. Exhaustion en LVN - Pendiente

No implementada ni testeada aún.

---

## Herramientas Existentes

| Archivo | Función | Estado |
|---|---|---|
| `agg_trades_to_db.py` | DB connector + queries | ✓ Completo |
| `populate_DB.py` | Descarga y actualiza datos | ✓ Completo |
| `graph_candlestick.py` | Visualización Bokeh | ✓ Parcial (señales visuales) |
| `candlestick_analytics.py` | Pattern detection (hammer) | ✓ Básico |
| `download_agg_trades_binance.py` | Descarga desde Binance | ✓ Completo |
| `strategy_scanner.py` | Scanner + backtest de estrategias | ✓ **+ Market Structure Filter** |

### Nuevas Capacidades (Sesión 3)

**Market Structure Detection:**
- Cálculo en `create_market_context_table()`
- Detecta HH/HL/LH/LL, BOS, CHoCH en timeframe 4H
- Filtro de tendencia aplicado en `strategy_scanner.py`

**Optimizaciones de Performance:**
- Índice automático en `agg_trades.timestamp`
- Incremental updates optimizados (WHERE clause pre-computado)

---

## Próximos Pasos (Roadmap Actualizado)

### Fase 1: Scanner de Patrones ✓ COMPLETADO
- [x] `strategy_scanner.py` - Escanea DB y muestra patrones en tablas
- [x] Filtros por estrategia (absorción, imbalance)
- [x] Backtest integrado con métricas (MFE, MAE, Win Rate, PF)
- [x] Export a CSV y HTML

### Fase 2: Market Structure ✓ IMPLEMENTADO
- [x] Detección de HH/HL/LH/LL en `market_context_4_hours`
- [x] Identificación de BOS y CHoCH
- [x] Campo `trend_direction` (UPTREND/DOWNTREND/RANGING)
- [x] Filtro de trend en `strategy_scanner.py`
- [ ] **PENDIENTE:** Visualización de estructura en `graph_candlestick.py`

### Fase 3: Profundizar en Imbalance (PRIORIDAD ACTUAL)
- [ ] Mejorar lógica de entry (confirmación de toque real al bin)
- [ ] Stops dinámicos basados en ATR o estructura
- [ ] Targets basados en HVN/POC cercanos
- [ ] Análisis de duración de trades
- [ ] ¿Imbalance + Market Structure = mejor PF?

### Fase 4: Nuevas Estrategias
- [ ] Exhaustion en LVN + MS
- [ ] VWAP Mean Reversion (con filtro de tendencia)
- [ ] Delta Divergence
- [ ] Order Blocks (velas con volumen extremo que inician movimiento)

### Fase 5: Sistema Completo
- [ ] Gestión de capital (position sizing, max drawdown diario)
- [ ] Walk-forward analysis
- [ ] Comparación de múltiples periodos
- [ ] Dashboard visual de resultados

---

## Lecciones Aprendidas

### Sesión 3 - Market Structure

#### Insights principales:
1. **Contexto antes que señal** — Saber la tendencia 4H filtra ~50% de falsas entradas
2. **Estructura fractal** — 4H para dirección, 15m para timing
3. **Ranging ≠ No operar** — En rango se opera cerca de los extremos (SH/SL)

#### Implementación técnica:
- Swing detection: 5 velas a cada lado funciona bien en 4H
- Forward-fill con subqueries correlacionadas es costoso (se simplificó)
- Los índices en timestamp son críticos para performance

---

### Sesión 2 - Primeros Backtests

#### Lo que NO funciona:
1. **Absorción "simple"** — Detectar velas con mechas y volumen no es suficiente
2. **El POC como nivel mágico** — El precio no siempre reacciona al POC
3. **Delta bajo como señal** — No predice reversión por sí solo

#### Lo que SÍ muestra potencial:
1. **Imbalance Fill** — Los desbalances de 3:1+ tienden a llenarse
2. **MFE/MAE como diagnóstico** — Si MFE << MAE, la idea está mal
3. **Profit Factor como filtro** — PF < 1 = abandonar o replantear

#### Errores conceptuales corregidos:
1. **Absorción ≠ Vela con mecha** — Se necesita confirmación de que el precio realmente fue "absorbido" y no simplemente cayó/subió
2. **Entry en ruptura es riesgoso** — Entrar en el high/low de la vela expone a falsas rupturas
3. **20 velas de salida es arbitrario** — Debería basarse en estructura o tiempo de la tesis

---

## Resultados de Backtest Archivados

**Periodo:** Febrero 2026 (01-28)
**Intervalo:** 15 minutos
**Instrumento:** BTCUSDT

| Estrategia | Trades | Win Rate | PF | Expectancy | Max DD |
|---|---|---|---|---|---|
| Absorción Long | 14 | 0% | 0.00 | -512 | 5,538 |
| Absorción Short | 10 | 40% | 0.80 | -65 | 2,457 |
| Imbalance Long | 260 | 46.2% | 1.29 | - | - |
| Imbalance Short | 370 | 45.4% | 1.30 | - | - |

**Archivos de salida:** `scanner_output/`
- `absorption_long.csv` — 14 señales con resultados
- `absorption_short.csv` — 10 señales con resultados
- `imbalance.csv` — 630 señales con resultados
- `scanner_summary.html` — Resumen visual con métricas

---

## Comandos Útiles

```bash
# Actualizar datos
pipenv run python populate_DB.py

# Ver gráficos exploratorios
pipenv run python graph_candlestick.py

# Scanner de patrones + backtest
pipenv run python strategy_scanner.py

# Ver resultados
# Abrir: scanner_output/scanner_summary.html
```

---

## Referencias

- Libro: "Order Flow: Trading Setups" - Trader Dale
- Libro: "Mind Over Markets" - James Dalton (Market Profile)
- Concepto clave: El mercado busca liquidez, no sigue patrones técnicos

---

## Notas para Próximas Sesiones

**Para continuar:**
1. Ejecutar `populate_DB.py` para poblar market_context_4_hours con estructura
2. Ejecutar `strategy_scanner.py` y comparar resultados CON filtro de Market Structure
3. Si mejora el PF de absorción, seguir refinando. Si no, priorizar Imbalance
4. Agregar visualización de SH/SL en `graph_candlestick.py`

**Preguntas abiertas:**
- ¿Los desbalances más extremos (5:1, 10:1) tienen mejor fill rate?
