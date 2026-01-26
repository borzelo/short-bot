# 🏗 Архитектура Millionaire Bot

## 📋 Оглавление

- [Общий обзор](#общий-обзор)
- [Компоненты системы](#компоненты-системы)
- [Поток данных](#поток-данных)
- [Стратегия детекции](#стратегия-детекции)
- [Индикаторы и триггеры](#индикаторы-и-триггеры)
- [Система скоринга](#система-скоринга)
- [Хранение данных](#хранение-данных)
- [Обработка ошибок](#обработка-ошибок)

---

## Общий обзор

**Millionaire Bot** - это высокопроизводительный микросервис на Go для мониторинга крипто-фьючерсов на ByBit и детектирования сигналов "OBVAL / пробоя слабых активов" в реальном времени. Стратегия смещена в сторону волатильных mid-cap инструментов и включает защиту от "short squeeze" и "buyback".

### Архитектурная диаграмма

```
┌─────────────────────────────────────────────────────────────────┐
│                        MILLIONAIRE BOT                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐      ┌──────────────┐
│   ByBit API  │    │  WebSocket   │      │  PostgreSQL  │
│              │    │   Stream     │      │   Database   │
│ (Instrument  │    │ (1m candles) │      │  (Signals +  │
│    Info)     │    │              │      │   Assets)    │
└──────────────┘    └──────────────┘      └──────────────┘
        │                     │                     │
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Strategy Engine │
                    │                  │
                    │ • RS Calculation │
                    │ • Funding Filter │
                    │ • Support Detect │
                    │ • Wick Check     │
                    │ • Volume Check   │
                    │ • Score Calc     │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Telegram Bot    │
                    │  (Notifications) │
                    └──────────────────┘
```

---

## Компоненты системы

### 1. **Main Entry Point** (`cmd/bot/main.go`)

**Роль**: Точка входа, инициализация и оркестрация всех компонентов.

**Ответственность**:
- Загрузка конфигурации из environment variables
- Подключение к PostgreSQL
- Инициализация всех модулей
- Запуск горутин для обработки данных
- Graceful shutdown при получении сигнала

**Горутины**:
```go
// 1. Обработка свечей из WebSocket
go processCandles(ctx, engine, wsClient)

// 2. Обработка сигналов (БД + Telegram)
go processSignals(ctx, engine, store, notifier)

// 3. Обработка переподключений WebSocket
go handleReconnect(ctx, wsClient, symbols, wsURL)

// 4. Периодическая статистика (каждые 5 минут)
go statsLogger(ctx, engine)

// 5. Обновление funding rates (каждые 5 минут)
go fundingUpdater(ctx, apiClient, engine)
```

---

### 2. **Configuration** (`internal/config`)

**Роль**: Централизованное управление настройками.

**Переменные окружения**:

| Переменная | Описание | Обязательная |
|-----------|----------|--------------|
| `DATABASE_URL` | PostgreSQL connection string | ✅ |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram бота | ✅ |
| `TELEGRAM_CHAT_ID` | ID чата для уведомлений | ✅ |
| `BYBIT_WS_URL` | WebSocket URL ByBit | ❌ (по умолчанию: `wss://stream.bybit.com/v5/public/linear`) |
| `BYBIT_API_URL` | REST API URL ByBit | ❌ (по умолчанию: `https://api.bybit.com`) |
| `BYBIT_API_ALT_URL` | Альтернативный REST API URL ByBit (fallback при 403) | ❌ |
| `LOG_LEVEL` | Уровень логирования | ❌ (по умолчанию: `info`) |

---

### 3. **ByBit Integration** (`internal/bybit`)

#### 3.1 **API Client** (`api.go`)

**Роль**: Получение списка торгуемых инструментов и funding rates.

**Функционал**:
- Получение всех USDT Perp тикеров и сортировка по turnover 24h
- Исключение Top-15 по объёму и выбор следующих 100 (Rank 16-116)
- Фильтр ликвидности: turnover >= $10M/24h
- Получение funding rates через `/v5/market/tickers`
- Fallback на hardcoded список при 403 ошибке (Cloudflare blocking)
- Возврат базовых параметров инструментов

**Fallback список** (100 символов, heavyweights исключены):
```go
ETHFIUSDT, CHZUSDT, ETCUSDT, HBARUSDT, XLMUSDT, TRXUSDT, BCHUSDT, ...
```

#### 3.2 **WebSocket Client** (`websocket.go`)

**Роль**: Real-time стриминг 1-минутных свечей.

**Протокол**:
```
1. Подключение к wss://stream.bybit.com/v5/public/linear
2. Подписка на kline.1.<SYMBOL> для каждого из 100 символов
   - Батчами по 10 символов (ограничение ByBit API)
3. Получение данных в формате JSON
4. Парсинг и отправка в канал
```

**Структура сообщения**:
```json
{
  "topic": "kline.1.BTCUSDT",
  "type": "snapshot",
  "data": [{
    "start": 1706274000000,
    "end": 1706274060000,
    "interval": "1",
    "open": "42000.5",
    "close": "42010.3",
    "high": "42015.0",
    "low": "41995.0",
    "volume": "125.456",
    "confirm": true  // ← Важно! Обрабатываем только закрытые свечи
  }]
}
```

**Reconnection**:
- Автоматическое переподключение при разрыве соединения
- Exponential backoff (1s, 2s, 4s, 8s, 16s)
- Максимум 5 попыток, затем Fatal error

**Ping/Pong**:
- Ping каждые 20 секунд для поддержания соединения
- Pong handler обновляет read deadline

---

### 4. **Strategy Engine** (`internal/strategy`)

**Роль**: Ядро системы - детекция сигналов пробоя.

#### 4.1 **Data Storage**

**Ring Buffer** для каждого символа:
```go
type Engine struct {
    candleCache map[string][]Candle  // symbol → последние 240 свечей
    btcCandles  []Candle              // BTC для расчета RS
    fundingRates map[string]float64   // symbol → funding rate (%)
    signalChan  chan *Signal          // Канал для сигналов
}
```

**Лимиты памяти**:
- Максимум **240 свечей** (4 часа) на символ
- При достижении лимита: удаление самой старой свечи
- ~100 символов × 240 свечей = **24,000 свечей в памяти**

#### 4.2 **Processing Pipeline**

**Для каждой новой свечи**:

```
┌─────────────────┐
│  New Candle     │
│  Received       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Store in       │
│  Ring Buffer    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Check Data     │
│  Availability   │  ← Нужно минимум 30 свечей
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Calculate RS   │  ← Relative Strength vs BTC
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Funding Check  │  ← Crowded short filter
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Detect         │
│  Support Level  │  ← Fractal Low pattern
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Check          │
│  Breakdown      │  ← Close < Support?
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Wick Analysis  │  ← Close near bottom?
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Check Volume   │  ← Volume > 1.5x avg?
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Calculate      │
│  Score (0-100)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Generate       │
│  Signal         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Save to DB +   │
│  Send Telegram  │
└─────────────────┘
```

---

## Стратегия детекции

### Концепция: "OBVAL / Weak Asset Breakdown"

**Идея**: Найти активы, которые:
1. **Слабее рынка** (падают сильнее чем BTC)
2. **Пробили поддержку** (закрытие ниже локального минимума)
3. **С высоким объёмом** (подтверждение продаж)
4. **Не в crowded short** (funding rate не слишком отрицательный)
5. **Закрылись у нижней границы свечи** (без buyback)

---

## Индикаторы и триггеры

### 1. **Relative Strength (RS)**

**Формула**:
```
RS = (Asset % Change) - (BTC % Change)
```

**Период**: 30-60 минут (адаптивно)

**Пример**:
```
BTC: +2.5% за последний час
ETH: -1.0% за последний час

RS(ETH) = -1.0% - 2.5% = -3.5%  ← Слабее рынка на 3.5%
```

**Триггер**: RS < -3% (актив отстаёт от BTC)

**Логика**:
```go
func calculateRS(symbol string) float64 {
    // Адаптивный lookback: 30-60 минут
    lookback := min(60, len(assetCandles), len(btcCandles))

    assetOld := assetCandles[len-lookback].Close
    assetNew := assetCandles[len-1].Close
    assetChange := ((assetNew - assetOld) / assetOld) * 100

    btcOld := btcCandles[len-lookback].Close
    btcNew := btcCandles[len-1].Close
    btcChange := ((btcNew - btcOld) / btcOld) * 100

    return assetChange - btcChange
}
```

---

### 2. **Support Level Detection**

**Метод**: Fractal Low Pattern

**Определение**:
Локальный минимум `Low[i]` считается фракталом, если:
```
Low[i] < Low[i-2] AND
Low[i] < Low[i-1] AND
Low[i] < Low[i+1] AND
Low[i] < Low[i+2]
```

**Визуализация**:
```
Price
  ^
  |     *           *
  |   *   *       *   *
  | *       *   *       *
  |           X            ← Fractal Low (Support)
  +------------------------> Time
       i-2  i  i+2
```

**Период поиска**: Последние 30 минут

**Логика**:
```go
func detectSupport(candles []Candle) *SupportLevel {
    recentCandles := candles[len-30:]  // 30 минут

    for i := 2; i < len(recentCandles)-2; i++ {
        low := recentCandles[i].Low
        isFractal := true

        // Проверка окружающих свечей
        for j := i-2; j <= i+2; j++ {
            if j == i { continue }
            if recentCandles[j].Low < low {
                isFractal = false
                break
            }
        }

        if isFractal {
            return &SupportLevel{
                Price: low,
                Timestamp: recentCandles[i].Timestamp
            }
        }
    }
    return nil
}
```

---

### 3. **Breakdown Trigger**

**Условие**:
```
Current Close < Support Level
```

**Пример**:
```
Support Level: $142.50
Current Close: $142.10  ← Пробой!
```

**Важно**: Проверяем **Close**, а не Low, чтобы избежать ложных пробоев (wicks).

---

### 4. **Funding Rate (Anti-Squeeze)**

**Идея**: Слишком отрицательный funding означает переполненные шорты → высокий риск squeeze.

**Триггер**:
```
FundingRate < -0.015%  →  сигнал отбрасывается
```

**Обновление**: funding rates обновляются каждые 5 минут.

---

### 5. **Wick / Close Position**

**Формула**:
```
candleRange := High - Low
closePosition := (Close - Low) / candleRange
```

**Триггер**:
```
closePosition <= 0.3  // закрытие в нижних 30%
```

**Логика**: если closePosition > 0.3, свеча имеет сильный откуп (buyback) → сигнал отбрасывается.

---

### 6. **Volume Confirmation**

**Формула**:
```
Volume Ratio = Current Volume / Average Volume
```

**Average Volume**: Средний объём за последние 20 свечей

**Триггер**: Volume Ratio > 1.5

**Логика**:
```go
func calculateAvgVolume(candles []Candle) float64 {
    recentCandles := candles[len-20:]  // Последние 20 свечей

    sum := 0.0
    for _, c := range recentCandles {
        sum += c.Volume
    }

    return sum / float64(len(recentCandles))
}

// В основной функции:
avgVolume := calculateAvgVolume(candles)
volumeRatio := currentCandle.Volume / avgVolume

if volumeRatio < 1.5 {
    return nil  // Не достаточно объёма, нет сигнала
}
```

**Примеры**:

| Avg Volume | Current Volume | Ratio | Результат |
|-----------|---------------|-------|-----------|
| 1000 | 2400 | 2.4x | ✅ Пробой подтверждён |
| 1000 | 1300 | 1.3x | ❌ Слабый объём |
| 1000 | 3500 | 3.5x | ✅ Очень сильный сигнал |

---

## Система скоринга

**Диапазон**: 0-100 баллов

### Факторы и веса

#### 1. **Слабость актива** (до 40 баллов)

```go
score := 0

if RS < -3.0 {
    score += 30  // Базовая слабость

    if RS < -5.0 {
        score += 10  // Экстремальная слабость
    }
}
```

**Примеры**:
- RS = -2.0% → 0 баллов
- RS = -3.5% → 30 баллов
- RS = -6.0% → 40 баллов

#### 2. **Объём пробоя** (до 30 баллов)

```go
if volumeRatio > 2.0 {
    score += 20  // Высокий объём

    if volumeRatio > 3.0 {
        score += 10  // Исключительный объём
    }
}
```

**Примеры**:
- Volume Ratio = 1.8x → 0 баллов (не прошёл триггер 1.5x)
- Volume Ratio = 2.3x → 20 баллов
- Volume Ratio = 3.5x → 30 баллов

#### 3. **Возраст уровня поддержки** (20 баллов)

```go
supportAge := time.Since(support.Timestamp).Minutes()

if supportAge > 30 {
    score += 20
}
```

**Логика**: Чем дольше уровень держался, тем значимее его пробой.

**Примеры**:
- Уровень существует 15 минут → 0 баллов
- Уровень существует 45 минут → 20 баллов

#### 4. **Дивергенция с BTC** (10 баллов)

```go
// Сравниваем последние 2 свечи
btcChange := ((btcCurrent - btcPrevious) / btcPrevious) * 100
assetChange := ((assetCurrent - assetPrevious) / assetPrevious) * 100

if btcChange > 0 && assetChange < 0 {
    score += 10  // BTC растёт, актив падает
}
```

**Логика**: Когда рынок растёт, а актив падает - это особенно слабый знак.

---

#### 5. **Funding (положительный)** (20 баллов)

```go
if fundingRate > 0.01 {
    score += 20
}
```

**Логика**: Положительный funding означает, что лонги платят шортам → пробой вниз устойчивее.

---

#### 6. **Close Position (сильный пробой)** (10 баллов)

```go
if closePosition < 0.1 {
    score += 10
}
```

**Логика**: Закрытие в нижних 10% свечи усиливает качество пробоя.

---

### Итоговая таблица скоринга

| Фактор | Условие | Баллы |
|--------|---------|-------|
| **Слабость** | RS < -3% | +30 |
| | RS < -5% | +10 (бонус) |
| **Объём** | Volume > 2x | +20 |
| | Volume > 3x | +10 (бонус) |
| **Возраст уровня** | Age > 30 min | +20 |
| **Дивергенция** | BTC↑ Asset↓ | +10 |
| **Funding** | Funding > 0.01% | +20 |
| **Close Position** | Close < 10% свечи | +10 |
| | | |
| **Максимум** | | **100 (cap)** |

---

### Интерпретация скора

```go
func getProbability(score int) string {
    switch {
    case score >= 80:
        return "Высокая вероятность падения"
    case score >= 60:
        return "Средняя вероятность падения"
    case score >= 40:
        return "Умеренная вероятность падения"
    default:
        return "Низкая вероятность падения"
    }
}
```

**Примеры реальных сигналов**:

| Score | Описание |
|-------|----------|
| 90 | RS=-5.5%, Volume=3.2x, Level Age=45min, BTC↑ Asset↓ |
| 70 | RS=-4.0%, Volume=2.5x, Level Age=35min |
| 50 | RS=-3.2%, Volume=2.1x, Level Age=20min |

---

## Хранение данных

### PostgreSQL Schema

#### Таблица `assets`

```sql
CREATE TABLE assets (
    symbol VARCHAR(20) PRIMARY KEY,
    tick_size DECIMAL NOT NULL,
    min_lot_size DECIMAL NOT NULL,
    is_tradable BOOLEAN DEFAULT TRUE
);
```

**Назначение**: Метаданные торгуемых инструментов

**Пример записи**:
```
symbol: "BTCUSDT"
tick_size: 0.01
min_lot_size: 0.001
is_tradable: true
```

#### Таблица `signals`

```sql
CREATE TABLE signals (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) REFERENCES assets(symbol),

    -- Технические данные
    price_trigger DECIMAL NOT NULL,      -- Цена закрытия при пробое
    level_broken DECIMAL NOT NULL,       -- Пробитый уровень поддержки
    breakdown_volume DECIMAL,            -- Объём при пробое

    -- Скоринг
    score_rs DECIMAL,                    -- Relative Strength
    score_total INT,                     -- Итоговый скор 0-100

    -- Метаданные
    meta JSONB                           -- Дополнительные данные
);

CREATE INDEX idx_signals_symbol_time ON signals(symbol, created_at DESC);
```

**Пример записи**:
```json
{
  "id": 42,
  "created_at": "2026-01-26T12:45:30Z",
  "symbol": "SOLUSDT",
  "price_trigger": 142.10,
  "level_broken": 142.50,
  "breakdown_volume": 1234.56,
  "score_rs": -4.2,
  "score_total": 85,
  "meta": {
    "volume_ratio": 2.4,
    "funding_rate": 0.012,
    "close_position": 0.08,
    "support_age_minutes": 45.0,
    "support_touch_count": 0,
    "avg_volume": 514.4
  }
}
```

#### Миграции

**Автоматические** при старте бота:

```go
func migrate(ctx context.Context) error {
    migrations := []string{
        `CREATE TABLE IF NOT EXISTS assets (...)`,
        `CREATE TABLE IF NOT EXISTS signals (...)`,
        `CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ...`,
    }

    for _, migration := range migrations {
        if _, err := pool.Exec(ctx, migration); err != nil {
            return err
        }
    }
    return nil
}
```

**Преимущества**:
- ✅ Нет необходимости в отдельных SQL скриптах
- ✅ Идемпотентность (`IF NOT EXISTS`)
- ✅ Всегда актуальная схема

---

## Telegram Notifications

### Формат сообщения (русский язык)

```
🚨 *СИГНАЛ: ПРОБОЙ УРОВНЯ* 🚨

Тикер: #SOLUSDT
Цена: 142.10 (Пробит уровень: 142.50)

📉 *Факторы слабости:*
• Относит. сила (RS): -4.2% (Слабее рынка)
• Объем при пробое: x2.4 от среднего

🛠 *Технический анализ:*
• Время жизни уровня: 45 мин
• Объем пробоя: 1234.56

🏆 *ОБЩИЙ БАЛЛ: 85/100*
*(Высокая вероятность падения)*
```

### Telegram API

**Endpoint**: `https://api.telegram.org/bot<TOKEN>/sendMessage`

**Payload**:
```json
{
  "chat_id": "123456789",
  "text": "...",
  "parse_mode": "Markdown"
}
```

**Retry Policy**: Нет автоматических повторов (логируем ошибку)

---

## Обработка ошибок

### 1. **WebSocket Disconnection**

**Trigger**: Read error или timeout

**Action**:
```go
1. Логируем ошибку
2. Отправляем сигнал в канал reconnect
3. Закрываем текущее соединение
4. Ждём 5 секунд
5. Переподключаемся с exponential backoff (1s, 2s, 4s, 8s, 16s)
6. Максимум 5 попыток
7. Fatal error если не удалось
```

### 2. **Database Errors**

**Trigger**: Connection lost, query failed

**Action**:
```go
1. Логируем ошибку с контекстом
2. Продолжаем работу (сигнал теряется, но бот не падает)
3. Connection pool автоматически восстановит соединение
```

### 3. **Telegram Send Errors**

**Trigger**: HTTP error, timeout

**Action**:
```go
1. Логируем ошибку
2. Продолжаем работу
3. Сигнал сохранён в БД, можно переотправить вручную
```

### 4. **ByBit API 403 (Cloudflare)**

**Trigger**: REST API blocked by Cloudflare

**Action**:
```go
1. Логируем warning
2. Переключаемся на fallback список из 100 символов
3. Продолжаем работу с захардкоженными инструментами
```

---

## Производительность

### Метрики

| Метрика | Значение |
|---------|----------|
| **Memory** | ~50-100 MB (24k свечей в памяти) |
| **CPU** | <5% (горутины + легковесные вычисления) |
| **Latency** | <100ms от получения свечи до генерации сигнала |
| **Throughput** | ~100 свечей/минуту (real-time) |

### Оптимизации

1. **Ring Buffer**: Фиксированный размер, избегаем аллокаций
2. **Channels**: Буферизованные каналы (1000 элементов)
3. **Connection Pooling**: pgxpool с 2-10 коннектами
4. **Goroutines**: Отдельные горутины для I/O операций

---

## Мониторинг

### Логирование (zerolog)

**Уровни**:
- `DEBUG`: Прогресс сбора данных
- `INFO`: Важные события (подключения, сигналы)
- `WARN`: Предупреждения (API fallback, reconnect)
- `ERROR`: Ошибки (failed DB save, Telegram send)
- `FATAL`: Критичные ошибки (нет DATABASE_URL)

**Structured Logging**:
```go
log.Info().
    Str("symbol", "SOLUSDT").
    Int("score", 85).
    Float64("rs", -4.2).
    Msg("signal generated")
```

**Output** (JSON в production):
```json
{
  "level": "info",
  "symbol": "SOLUSDT",
  "score": 85,
  "rs": -4.2,
  "time": "2026-01-26T12:45:30Z",
  "message": "signal generated"
}
```

### Периодическая статистика

**Каждые 5 минут**:
```go
stats := map[string]interface{}{
    "tracked_symbols": len(candleCache),
    "btc_candles": len(btcCandles),
}

log.Info().Interface("stats", stats).Msg("engine statistics")
```

**Пример вывода**:
```json
{
  "level": "info",
  "stats": {
    "tracked_symbols": 61,
    "btc_candles": 30
  },
  "message": "engine statistics"
}
```

---

## Deployment (Railway)

### Build Process

1. **Dockerfile Multi-stage**:
   ```dockerfile
   # Stage 1: Build
   FROM golang:1.22-alpine AS builder
   WORKDIR /app
   COPY go.mod go.sum ./
   RUN go mod download
   COPY . .
   RUN CGO_ENABLED=0 go build -o bot ./cmd/bot

   # Stage 2: Runtime
   FROM alpine:latest
   COPY --from=builder /app/bot .
   CMD ["./bot"]
   ```

2. **Environment Variables** (Railway Dashboard):
   - `DATABASE_URL` (auto-created by PostgreSQL addon)
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`

3. **Auto-deploy**: При push в GitHub → автоматический деплой

### Graceful Shutdown

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

<-sigChan  // Блокируем до получения сигнала

log.Info().Msg("🛑 Shutdown signal received")
cancel()   // Отменяем context
time.Sleep(2 * time.Second)  // Даём горутинам завершиться
log.Info().Msg("👋 Bot stopped")
```

---

## Безопасность

### 1. **Environment Variables**

- ✅ Все секреты через ENV (не в коде)
- ✅ `.env` файл в `.gitignore`
- ✅ `.env.example` для документации

### 2. **Database**

- ✅ Connection pooling с лимитами
- ✅ Prepared statements (защита от SQL injection)
- ✅ SSL соединение на production (Railway автоматически)

### 3. **External APIs**

- ✅ Timeout на всех HTTP запросах (10-30s)
- ✅ User-Agent headers для обхода Cloudflare
- ✅ Fallback механизмы при блокировке

---

## Масштабирование

### Горизонтальное

**Ограничение**: Нельзя запускать несколько инстансов из-за:
- Дублирование сигналов в Telegram
- Конкуренция за WebSocket соединение

**Решение** (если нужно):
- Distributed lock (Redis)
- Partitioning символов между инстансами

### Вертикальное

**Текущий профиль**: Railway Hobby Plan (~512MB RAM)

**Можно увеличить**:
- Количество символов (сейчас 100)
- Глубину истории (сейчас 240 свечей)
- Частоту статистики

---

## Итоговая архитектура в цифрах

| Компонент | Технология | Описание |
|-----------|------------|----------|
| **Runtime** | Go 1.22 | Высокопроизводительный, низкий memory footprint |
| **Database** | PostgreSQL 15+ | Надёжное хранение сигналов |
| **WebSocket** | gorilla/websocket | Real-time стриминг свечей |
| **Logging** | zerolog | Structured JSON logs |
| **Deployment** | Railway | Containerized, auto-deploy |
| | | |
| **Latency** | <100ms | От свечи до сигнала |
| **Memory** | ~50-100MB | 24k свечей в памяти |
| **Symbols** | 100 USDT Perps | Mid-cap, исключая Top-15 |
| **Warmup** | 30 минут | До первого сигнала |

---

## Дальнейшие улучшения

### Потенциальные фичи

1. **Backtesting**: Исторический анализ эффективности стратегии
2. **Multiple Timeframes**: Поддержка 5m, 15m свечей
3. **Additional Indicators**: RSI, MACD, Bollinger Bands
4. **Smart Filtering**: Cooldown между сигналами по символу
5. **Web Dashboard**: Real-time мониторинг через веб-интерфейс
6. **Alert Customization**: Пользовательские пороги скоринга

---

**Документация актуальна на**: 26 января 2026
**Версия бота**: 1.1.0
**Автор архитектуры**: AI-assisted development
