# Historical Data Import Tool

Утилита для импорта исторических данных из CSV.GZ файлов в таблицу `training_data`.

## Назначение

Обрабатывает готовые CSV файлы с историческими свечами и метриками, детектирует пробои поддержки, применяет фильтры качества и сохраняет в БД для обучения ML модели.

## Формат входных данных

### Структура файлов

```
data/
  BTCUSDT_processed.csv.gz   ← Обязательно! (для RS calculation)
  ETHUSDT_processed.csv.gz
  SOLUSDT_processed.csv.gz
  ...
```

### Формат CSV (18 колонок)

```
timestamp,open,high,low,close,volume,funding_rate,open_interest,high_24h,low_24h,price_24h_pcnt,volume_usd,price_15m_max,price_15m_min,price_15m_close,price_60m_max,price_60m_min,price_60m_close
```

**Nullable поля:**
- `funding_rate` - если NaN, используется 0.0
- `open_interest` - если NaN, пропускаются OI фильтры
- `price_15m_*` - если NaN, outcomes не заполняются
- `price_60m_*` - если NaN, outcomes не заполняются

## Использование

### Сборка

```bash
cd cmd/import_historical
go build -o import_historical
```

### Запуск

```bash
./import_historical \
  -dir /path/to/csv/files \
  -db "postgresql://user:pass@localhost:5432/millionaire" \
  -workers 10 \
  -debug
```

### Параметры

| Флаг | По умолчанию | Описание |
|------|--------------|----------|
| `-dir` | `.` | Директория с CSV.GZ файлами |
| `-db` | `$DATABASE_URL` | PostgreSQL connection URL |
| `-workers` | `10` | Количество параллельных обработчиков |
| `-debug` | `false` | Включить debug логирование |

### Пример

```bash
# С переменной окружения DATABASE_URL
export DATABASE_URL="postgresql://postgres:password@localhost:5432/millionaire"
./import_historical -dir ./historical_data -workers 20

# Или явно указать БД
./import_historical \
  -dir ./historical_data \
  -db "postgresql://postgres:password@localhost:5432/millionaire" \
  -workers 10
```

## Алгоритм обработки

### 1. Инициализация

- Загружает `BTCUSDT_processed.csv.gz` полностью в память
- Строит map для быстрого поиска BTC свечей по timestamp
- Строит RingBuffer для BTC (для divergence calculation)

### 2. Параллельная обработка

- Находит все `*_processed.csv.gz` файлы (кроме BTC)
- Запускает N воркеров (по умолчанию 10)
- Каждый воркер обрабатывает один файл независимо

### 3. Обработка одного символа

#### a. Warm-up период
- Пропускает первые 200 свечей
- Нужно для заполнения RingBuffer (MA200, Z-Score, Support detection)

#### b. Основной цикл (строка за строкой)
```
Для каждой свечи с i=200 до конца:
  1. Добавить свечу в RingBuffer
  2. Найти соответствующую BTC свечу по timestamp
  3. Детектировать пробой поддержки:
     - DetectSupport() из strategy пакета
     - Проверка candle.Close < support.Price
     - ConfirmNoBounceback()
  4. Если пробой НЕ детектирован → пропустить
  5. Рассчитать 35+ features из CSV + buffer
  6. Применить quality filters (RS, Volume, Funding, etc.)
  7. Создать TrainingData запись:
     - Features из расчетов
     - Outcomes из CSV (price_15m_*, price_60m_*)
     - is_shadow_mode = !passed_filters
  8. Добавить в batch (1000 записей)
  9. При заполнении batch → INSERT в БД
```

#### c. Финализация
- Flush остатков batch в БД
- Логирование статистики (breakdowns, elite, shadow)

## Features (35+ индикаторов)

### Из расчетов:
- **RS** - Relative Strength vs BTC (15min lookback)
- **Volume** - ratio, z_score, mean, stddev, avg_volume
- **Close Position** - позиция Close в диапазоне свечи
- **Support** - age_minutes, touch_count, is_consolidation_break
- **MA200** - sma200_deviation
- **Change** - change_1h, btc_divergence
- **Time** - hour_of_day (UTC), day_of_week
- **Candle** - OHLCV, range

### Из CSV:
- **Funding Rate** - funding_rate (NaN → 0.0)
- **24h Stats** - high_24h, low_24h, price_24h_pcnt, ndr_24h, dist_from_high_pct
- **Smart Pump** - is_smart_pump_rollover
- **Candle Type** - is_red_candle

### Пропущенные (недостаточно данных):
- **OI Delta** - oi_delta_pct = 0.0 (нет истории OI в CSV)
- **OI Flags** - is_aggressive_short = false, is_long_exit = false

## Quality Filters (Data Mining Thresholds)

Используются **мягкие** пороги для сбора разнообразных примеров:

| Фильтр | Порог | Обоснование |
|--------|-------|-------------|
| RS Threshold | >= 0.0% | Пусть модель видит активы наравне с BTC |
| Volume Ratio | >= 1.2x | Нужны примеры вялых пробоев без объема |
| Funding Floor | < -0.03% | Риск short squeeze |
| Oversold 24h | >= -40% | Dead cat bounce паттерн |
| Max Pump 24h | <= 30% | Фильтр экстремальной волатильности |
| MA Extension | <= 8% | Mean reversion риск |
| Close Position | <= 30% | Пробой с conviction |
| Support Age | >= 60 min (или 30 min если 3+ touches) | Динамический порог |
| Volatility NDR | >= 3% | Минимальная волатильность |
| Smart Pump Rollover | false | Риск разворота |
| OI Long Exit | false | Риск squeeze (пропускается если OI=NaN) |

**is_shadow_mode = true** если НЕ прошел хотя бы один фильтр.

## Производительность

### Оценка времени

**Для одного символа:**
- 260,000 строк (6 месяцев)
- Warm-up: 200 строк пропущено
- Обработка: ~259,800 строк
- Скорость: ~5,000-10,000 строк/сек (зависит от I/O БД)
- **Время: 25-50 секунд на символ**

**Для 200 символов:**
- При 10 workers: ~8-16 минут
- При 20 workers: ~4-8 минут

### Batch Insert

- Размер batch: 1000 записей
- Это ускоряет запись в БД в ~100x по сравнению с построчной вставкой

### Параллелизация

- Worker pool обрабатывает файлы независимо
- Рекомендуется: 10-20 workers (зависит от CPU cores и DB connections)
- Не превышайте `max_conns` в PostgreSQL pool

## Логирование

### Info уровень (по умолчанию)

```
2026-01-29T12:00:00Z info starting historical data import data_dir=./data workers=10
2026-01-29T12:00:01Z info loading BTC reference data...
2026-01-29T12:00:03Z info BTC data loaded btc_rows=262021
2026-01-29T12:00:03Z info found symbol files total_symbols=199
2026-01-29T12:00:08Z info symbol processed symbol=ETHUSDT rows=262021 breakdowns=42 elite=18 shadow=24 duration=5.2s progress=1 total=199
2026-01-29T12:00:12Z info symbol processed symbol=SOLUSDT rows=262021 breakdowns=56 elite=31 shadow=25 duration=4.8s progress=2 total=199
...
2026-01-29T12:15:30Z info ✅ import completed symbols=199 total_breakdowns=8432 elite_signals=3891 shadow_signals=4541 total_duration=15m30s
```

### Debug уровень

```bash
./import_historical -dir ./data -debug
```

Дополнительно логирует:
- Missing BTC timestamps (gaps)
- Детали расчетов
- Ошибки парсинга

## Обработка ошибок

### Ошибки файлов

- **BTC file missing** → FATAL (нельзя рассчитать RS)
- **Invalid CSV format** → SKIP symbol, continue
- **Insufficient data (< 200 rows)** → SKIP symbol, continue

### Ошибки данных

- **Missing BTC timestamp** → SKIP candle, continue (логируется в debug)
- **NaN funding_rate** → Use 0.0
- **NaN open_interest** → Skip OI filters (is_aggressive_short = false)
- **NaN outcomes** → Save with NULL outcomes (worker может заполнить позже)

### Ошибки БД

- **Connection failed** → FATAL
- **Batch insert failed** → FAIL symbol, continue with next

## Проверка результатов

### SQL запросы

```sql
-- Общая статистика
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE is_shadow_mode = false) AS elite_signals,
  COUNT(*) FILTER (WHERE is_shadow_mode = true) AS shadow_signals,
  COUNT(DISTINCT symbol) AS unique_symbols
FROM training_data;

-- По символам
SELECT
  symbol,
  COUNT(*) AS breakdowns,
  COUNT(*) FILTER (WHERE is_shadow_mode = false) AS elite,
  COUNT(*) FILTER (WHERE is_shadow_mode = true) AS shadow,
  ROUND(AVG((features->>'rs')::float), 2) AS avg_rs,
  ROUND(AVG((features->>'volume_z_score')::float), 2) AS avg_vol_z
FROM training_data
GROUP BY symbol
ORDER BY breakdowns DESC
LIMIT 20;

-- Проверка outcomes
SELECT
  COUNT(*) FILTER (WHERE price_15m_close IS NOT NULL) AS with_15m_outcomes,
  COUNT(*) FILTER (WHERE price_60m_close IS NOT NULL) AS with_60m_outcomes,
  COUNT(*) FILTER (WHERE price_15m_close IS NULL) AS missing_15m,
  COUNT(*) FILTER (WHERE price_60m_close IS NULL) AS missing_60m
FROM training_data;

-- Распределение по времени суток (UTC)
SELECT
  (features->>'hour_of_day')::int AS hour_utc,
  COUNT(*) AS breakdowns
FROM training_data
GROUP BY hour_utc
ORDER BY hour_utc;
```

## Troubleshooting

### Проблема: "failed to load BTC data"

**Причина:** Файл `BTCUSDT_processed.csv.gz` не найден или поврежден.

**Решение:**
```bash
ls -lh /path/to/data/BTCUSDT_processed.csv.gz
gunzip -t /path/to/data/BTCUSDT_processed.csv.gz  # test integrity
```

### Проблема: "invalid header: expected 18 columns"

**Причина:** CSV файл имеет неправильный формат.

**Решение:**
```bash
gunzip -c file.csv.gz | head -1  # Check header
# Should be: timestamp,open,high,low,close,volume,funding_rate,...
```

### Проблема: Слишком медленная обработка

**Причина:** Медленная запись в БД или мало workers.

**Решение:**
```bash
# Увеличить количество workers
./import_historical -dir ./data -workers 20

# Проверить DB pool settings в internal/db/db.go:
# MaxConns должно быть >= workers
```

### Проблема: "connection pool exhausted"

**Причина:** Слишком много workers для DB pool.

**Решение:**
```bash
# Уменьшить workers или увеличить pool в db.go
./import_historical -dir ./data -workers 5
```

## Следующие шаги

После импорта данных:

1. **Проверить данные:**
   ```sql
   SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM training_data;
   ```

2. **Экспортировать для ML:**
   ```bash
   # См. раздел "Feature Engineering для ML" в TRAINING_DATA_COLLECTION.md
   python scripts/export_training_data.py
   ```

3. **Обучить модель:**
   ```python
   # sklearn, xgboost, или другой ML framework
   model.fit(X_train, y_train)
   ```

4. **Интегрировать предсказания:**
   - Добавить поле `ml_prediction` в production
   - Использовать для принятия решений о сигналах
