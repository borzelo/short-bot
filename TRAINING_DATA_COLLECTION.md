# Training Data Collection System (v1.9.0)

> **Цель:** Сбор датасета для обучения ML prediction model
> **Подход:** Capture ALL breakdowns (structure), then filter by quality

---

## Концепция

### Проблема эвристического скоринга

Текущая система (v1.8.0) использует hardcoded веса для оценки сигналов:
- RS < -3% → +30 баллов
- Volume Z-Score > 2.0 → +10 баллов
- Funding > 0.01% → +20 баллов
- ...

**Недостатки:**
1. Субъективные веса (почему 30, а не 25?)
2. Не учитывает взаимосвязи между факторами
3. Одинаковые веса при разных рыночных условиях

### Решение: ML Prediction Model

**Что нужно:**
1. **Качественный датасет** с историческими сигналами и их отработкой
2. **Разнообразные примеры** — как хорошие, так и плохие сигналы
3. **Полный набор features** — все индикаторы в момент T=0
4. **Известные outcomes** — что произошло через 15/60 минут

**Что получим:**
- Модель сама выучит оптимальные веса
- Учет нелинейных зависимостей
- Адаптация к меняющемуся рынку (retrain на свежих данных)

---

## Архитектура v1.9.0

### Инверсия логики: Structure → Quality

#### Старый подход (v1.8.0 — Trading Engine):
```
Candle → Quality Filters (RS, Volume, Funding...) → Structure (Support, Breakdown) → Save if passed
```

**Проблема:** Собираем только "элитные" сигналы. Модель не видит, почему другие паттерны плохие.

#### Новый подход (v1.9.0 — Data Mining Engine):
```
Candle → Structure (Support, Breakdown) → SAVE TO DB (все события!) → Quality Filters → Mark shadow_mode
```

**Преимущества:**
- Модель видит ВСЕ пробои, включая ложные
- Учится отличать хорошие сигналы от плохих
- Больше данных для обучения

---

## Database Schema

### Таблица `training_data`

```sql
CREATE TABLE training_data (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) NOT NULL,

    -- ENTRY POINT (T=0)
    entry_price DECIMAL NOT NULL,
    entry_support_level DECIMAL,

    -- FEATURES (INPUTS) - На чем учим модель
    features JSONB NOT NULL,

    -- RESULTS AFTER 15 MINUTES (OUTPUTS)
    price_15m_max DECIMAL,    -- High за 15 мин (проверка SL)
    price_15m_min DECIMAL,    -- Low за 15 мин (проверка TP)
    price_15m_close DECIMAL,  -- Close через 15 мин

    -- RESULTS AFTER 60 MINUTES (OUTPUTS)
    price_60m_max DECIMAL,
    price_60m_min DECIMAL,
    price_60m_close DECIMAL,

    -- META INFORMATION
    is_shadow_mode BOOLEAN DEFAULT FALSE,  -- TRUE = не прошел фильтры
    ml_prediction FLOAT DEFAULT NULL       -- (Будущее) Предсказание модели
);

CREATE INDEX idx_training_pending ON training_data(created_at)
WHERE price_60m_close IS NULL;

CREATE INDEX idx_training_symbol ON training_data(symbol, created_at DESC);
```

### Поля `features` (JSONB)

Все индикаторы в момент детекции пробоя:

| Группа | Поля |
|--------|------|
| **Relative Strength** | `rs` |
| **Volume** | `volume_ratio`, `volume_z_score`, `volume_mean`, `volume_stddev`, `avg_volume` |
| **Funding** | `funding_rate` |
| **Candle Analysis** | `close_position`, `candle_high`, `candle_low`, `candle_open`, `candle_close`, `candle_volume`, `candle_range` |
| **Support Level** | `support_age_minutes`, `support_touch_count`, `is_consolidation_break` |
| **24h Statistics** | `ndr_24h`, `price_24h_pcnt`, `high_price_24h`, `low_price_24h`, `dist_from_high_pct` |
| **Open Interest** | `oi_delta_pct`, `is_aggressive_short`, `is_long_exit` |
| **Moving Averages** | `sma200_deviation` |
| **Volatility** | `change_1h` |
| **Market Context** | `btc_divergence`, `is_smart_pump_rollover`, `is_red_candle` |
| **Time Features** | `hour_of_day` (0-23), `day_of_week` (0-6) |

---

## Data Mining Thresholds

### Почему пороги мягче чем для трейдинга?

**Цель:** Показать модели разнообразные сценарии, включая плохие, чтобы она выучила, почему они плохие.

| Параметр | Trading (v1.8.0) | Data Mining (v1.9.0) | Обоснование |
|----------|------------------|----------------------|-------------|
| **RS Threshold** | -1.2% | **0.0%** | Пусть модель увидит активы наравне с BTC и поймет, почему их шортить нельзя |
| **Volume Ratio** | 1.8x | **1.2x** | Нужны примеры "вялых" пробоев без объема — это ложные сигналы |
| **Funding Floor** | -0.015% | **-0.03%** | Пусть попадут ситуации с легким перекосом в шорт (риск squeeze) |
| **Oversold 24h** | -25% | **-40%** | Модель выучит паттерн "Dead Cat Bounce" (отскок дохлой кошки) |
| **MA Extension** | 4% | **8%** | Модель научится предсказывать mean reversion (возврат к средней) |

### Pipeline сравнение

#### Trading Engine (v1.8.0):
```
1. Volatility Filter (NDR < 3%) → REJECT
2. Oversold Filter (-25%) → REJECT
3. Max Pump (30%) → REJECT
4. Smart Pump Near High → REJECT
5. MA Extension (4%) → REJECT
6. OI Long Exit → REJECT
7. RS (>= -1.2%) → REJECT
8. Funding (< -0.015%) → REJECT
9. Volume (< 1.8x) → REJECT
10. Close Position (> 30%) → REJECT
11. Support Detection → REJECT if none
12. Breakdown → REJECT if none
13. Score → Send to Telegram if >= 50
```

#### Data Mining Engine (v1.9.0):
```
1. Support Detection → SKIP if none
2. Breakdown → SKIP if none
3. 💾 SAVE TO training_data (все пробои!)
4. Quality Filters:
   - Volatility (NDR < 3%)
   - Oversold (-40%)
   - Max Pump (30%)
   - Smart Pump Near High
   - MA Extension (8%)
   - OI Long Exit
   - RS (>= 0.0%)
   - Funding (< -0.03%)
   - Volume (< 1.2x)
   - Close Position (> 30%)
   - Support Age (dynamic)
5. Mark is_shadow_mode = !passed_all_filters
6. If elite → also send to Telegram
```

---

## Использование

### 1. Сбор исторических данных

```go
// Создать data mining engine вместо обычного strategy engine
trainingEngine := training.NewDataMiningEngine(store)

// Подписаться на те же символы, что и в production
// Прогнать исторические свечи через trainingEngine.ProcessCandle()

// Результат: training_data заполняется ВСЕМИ пробоями (shadow + elite)
```

### 2. Background Worker (TODO)

**Задача:** Заполнить `price_15m_*` и `price_60m_*` через соответствующее время.

**Алгоритм:**
```go
// Каждые 5 минут:
1. SELECT * FROM training_data WHERE price_60m_close IS NULL AND created_at < NOW() - INTERVAL '60 minutes'
2. Для каждой строки:
   - Получить свечи за последние 60 минут от created_at
   - Заполнить price_15m_max/min/close (первые 15 свечей)
   - Заполнить price_60m_max/min/close (все 60 свечей)
   - UPDATE training_data SET ...
```

### 3. Feature Engineering для ML

```python
import pandas as pd
import psycopg2

# Загрузить датасет
conn = psycopg2.connect(DATABASE_URL)
df = pd.read_sql("""
    SELECT
        id,
        symbol,
        created_at,
        entry_price,
        features,  -- JSONB → распарсить в pandas
        price_15m_close,
        price_60m_close,
        is_shadow_mode
    FROM training_data
    WHERE price_60m_close IS NOT NULL  -- Только завершенные
""", conn)

# Распарсить JSONB features в колонки
features_df = pd.json_normalize(df['features'])
df = pd.concat([df.drop('features', axis=1), features_df], axis=1)

# Target variable (например, -1% за 15 минут)
df['target_15m'] = (df['price_15m_close'] - df['entry_price']) / df['entry_price']
df['is_profitable_15m'] = df['target_15m'] < -0.01  # Short profit > 1%

# Обучить модель
from sklearn.ensemble import RandomForestClassifier

X = df[['rs', 'volume_z_score', 'funding_rate', ...]]  # Все features
y = df['is_profitable_15m']

model = RandomForestClassifier()
model.fit(X_train, y_train)
```

---

## Преимущества подхода

| Аспект | Эвристика (v1.8.0) | ML (v1.9.0) |
|--------|-------------------|-------------|
| **Веса факторов** | Hardcoded (субъективно) | Выучены из данных |
| **Взаимосвязи** | Не учитываются | Автоматически детектируются |
| **Адаптация** | Требует ручной настройки | Retrain на свежих данных |
| **Обучение на ошибках** | Нет (видим только хорошие) | Да (shadow_mode) |
| **Нелинейные паттерны** | Сложно кодировать | Автоматически |

---

## Следующие шаги

### ✅ Completed (v1.9.0)
- [x] Создать таблицу `training_data`
- [x] Создать пакет `internal/training/`
- [x] Инвертировать логику (Structure → Quality)
- [x] Мягкие пороги для data mining
- [x] Сохранять ALL breakdowns с флагом `is_shadow_mode`
- [x] Добавить time features (hour_of_day, day_of_week)

### 🔲 TODO
- [ ] Background worker для заполнения 15m/60m outcomes
- [ ] Интеграция training engine в main.go (параллельно с trading engine?)
- [ ] Прогонка исторических данных за 6 месяцев
- [ ] Feature engineering скрипты (Python)
- [ ] Обучение первой версии модели
- [ ] Интеграция предсказаний модели в production

---

**Версия:** v1.9.0 — Training Data Collection System
**Дата:** 29 января 2026
**Автор:** Millionaire Bot Team
