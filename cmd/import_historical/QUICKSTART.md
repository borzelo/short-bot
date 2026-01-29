# Quick Start Guide

## Быстрый старт

### 1. Подготовка данных

Убедитесь что у вас есть CSV.GZ файлы в формате:
```
data/
  BTCUSDT_processed.csv.gz   ← Обязательно!
  ETHUSDT_processed.csv.gz
  SOLUSDT_processed.csv.gz
  ...
```

### 2. Сборка

```bash
cd cmd/import_historical
go build -o import_historical
```

### 3. Запуск

```bash
# Установить DATABASE_URL
export DATABASE_URL="postgresql://user:password@localhost:5432/millionaire"

# Запустить импорт
./import_historical -dir /path/to/csv/files -workers 10
```

### 4. Пример вывода

```
2026-01-29T14:00:00Z info starting historical data import data_dir=./data workers=10
2026-01-29T14:00:01Z info loading BTC reference data...
2026-01-29T14:00:03Z info BTC data loaded btc_rows=262021
2026-01-29T14:00:03Z info found symbol files total_symbols=199

2026-01-29T14:00:08Z info symbol processed symbol=ETHUSDT rows=262021 breakdowns=42 elite=18 shadow=24 duration=5.2s progress=1 total=199
2026-01-29T14:00:12Z info symbol processed symbol=SOLUSDT rows=262021 breakdowns=56 elite=31 shadow=25 duration=4.8s progress=2 total=199
...

2026-01-29T14:15:30Z info ✅ import completed symbols=199 total_breakdowns=8432 elite_signals=3891 shadow_signals=4541 total_duration=15m30s
```

### 5. Проверка результатов

```sql
-- Общая статистика
SELECT
  COUNT(*) AS total_records,
  COUNT(*) FILTER (WHERE is_shadow_mode = false) AS elite_signals,
  COUNT(*) FILTER (WHERE is_shadow_mode = true) AS shadow_signals,
  COUNT(DISTINCT symbol) AS unique_symbols
FROM training_data;

-- Примеры записей
SELECT
  id,
  symbol,
  entry_price,
  entry_support_level,
  is_shadow_mode,
  features->>'rs' AS rs,
  features->>'volume_z_score' AS vol_z,
  price_15m_close,
  price_60m_close
FROM training_data
LIMIT 10;
```

### 6. Производительность

**Ожидаемое время:**
- 1 символ (~260k строк) = 25-50 секунд
- 200 символов с 10 workers = 8-16 минут
- 200 символов с 20 workers = 4-8 минут

**Рекомендации:**
- Используйте SSD для CSV файлов (I/O интенсивная задача)
- Workers = количество CPU cores (или чуть больше)
- Не превышайте `max_conns` в PostgreSQL pool (по умолчанию 10)

## Параметры

| Флаг | По умолчанию | Описание |
|------|--------------|----------|
| `-dir` | `.` | Директория с CSV.GZ файлами |
| `-db` | `$DATABASE_URL` | PostgreSQL connection string |
| `-workers` | `10` | Количество параллельных обработчиков |
| `-debug` | `false` | Включить debug логирование |

## Troubleshooting

### Ошибка: "failed to load BTC data"
```bash
# Проверить что BTCUSDT файл существует
ls -lh /path/to/data/BTCUSDT_processed.csv.gz
```

### Ошибка: "connection pool exhausted"
```bash
# Уменьшить workers или увеличить DB pool
./import_historical -dir ./data -workers 5
```

### Медленная обработка
```bash
# Увеличить workers (если есть свободные CPU)
./import_historical -dir ./data -workers 20
```

## Что дальше?

После успешного импорта:

1. **Экспортировать для ML:**
   ```sql
   COPY (
     SELECT
       symbol,
       created_at,
       entry_price,
       features,
       price_15m_close,
       price_60m_close,
       is_shadow_mode
     FROM training_data
     WHERE price_60m_close IS NOT NULL
   ) TO '/tmp/training_dataset.csv' WITH CSV HEADER;
   ```

2. **Обучить модель (Python):**
   ```python
   import pandas as pd
   df = pd.read_csv('training_dataset.csv')
   # Feature engineering + model training
   ```

3. **Интегрировать в production:**
   - Добавить ML predictions в real-time engine
   - Использовать для принятия решений о сигналах
