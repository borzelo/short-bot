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
                    │ • Volatility Flt │  ← v1.3.0
                    │ • RS Calculation │
                    │ • Funding Filter │
                    │ • Support Detect │
                    │ • Wick Check     │
                    │ • Volume Check   │
                    │ • Signal Cooldown│
                    │ • Score Calc     │
                    │ • Pump Rollover  │  ← v1.3.0
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

// 3. Периодические задачи (статистика + ticker data)
go runPeriodicTasks(ctx, engine, updateTickerData)
```

> **Примечание**: Реконнект WebSocket теперь управляется внутри `WSClient` (SRP принцип).

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

**Роль**: Получение списка торгуемых инструментов, funding rates и 24h статистики.

**Функционал**:
- Получение всех USDT Perp тикеров и сортировка по turnover 24h
- Исключение Top-15 по объёму и выбор следующих 100 (Rank 16-116)
- Фильтр ликвидности: turnover >= $10M/24h
- Получение funding rates через `/v5/market/tickers`
- **v1.3.0**: Получение 24h статистики (`highPrice24h`, `lowPrice24h`, `price24hPcnt`) для фильтра волатильности и Pump Rollover
- **Multi-endpoint fallback** при 403 ошибке (Cloudflare blocking)
- Возврат базовых параметров инструментов

**Альтернативные API endpoints** (автоматический перебор):
```go
var alternativeAPIs = []string{
    "https://api.bytick.com",      // Alternative ByBit domain
    "https://api.bybit.nl",        // Netherlands region
    "https://api-demo.bybit.com",  // Demo API (real market data)
}
```

**Fallback для funding rates**: Если все API недоступны, возвращаются нулевые значения (сигналы работают, но без funding scoring).

#### 3.2 **WebSocket Client** (`websocket.go`)

**Роль**: Real-time стриминг 1-минутных свечей с автоматическим реконнектом.

**Протокол**:
```
1. Подключение к wss://stream.bybit.com/v5/public/linear
2. Подписка на kline.1.<SYMBOL> для каждого из 100 символов
   - Батчами по 10 символов (ограничение ByBit API)
3. Получение данных в формате JSON
4. Валидация и парсинг свечей
5. Отправка в канал
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

**Self-Healing Reconnection** (внутри WSClient):
```go
const (
    maxReconnectAttempts = 10
    baseReconnectDelay   = 1 * time.Second
    maxReconnectDelay    = 60 * time.Second
)
```
- Автоматическое переподключение при разрыве соединения
- Exponential backoff с лимитом 60 секунд
- Максимум 10 попыток, затем **graceful shutdown через fatalChan** (v1.7.0)
- **Не требует внешнего handleReconnect** — соответствует SRP

**Graceful Error Handling (v1.7.0):**
```go
// Вместо log.Fatal() — отправка ошибки в канал для graceful shutdown
if !c.reconnect() {
    err := fmt.Errorf("unable to maintain WebSocket after %d attempts", maxReconnectAttempts)
    c.fatalChan <- err  // main.go слушает этот канал
    return
}
```

**Валидация свечей**:
```go
func validateCandle(c models.Candle) error {
    if c.High < c.Low { return fmt.Errorf("high < low") }
    if c.Close > c.High || c.Close < c.Low { return fmt.Errorf("close outside range") }
    if c.Open > c.High || c.Open < c.Low { return fmt.Errorf("open outside range") }
    if c.Volume < 0 { return fmt.Errorf("negative volume") }
    if c.Open == 0 || c.High == 0 || c.Low == 0 || c.Close == 0 {
        return fmt.Errorf("zero price detected")
    }
    return nil
}
```

**Ping/Pong**:
- Ping каждые 20 секунд для поддержания соединения
- Pong handler обновляет read deadline (60 секунд)

---

### 4. **Strategy Engine** (`internal/strategy`)

**Роль**: Ядро системы - детекция сигналов пробоя.

#### 4.1 **Конфигурационные константы**

```go
const (
    MaxCandlesInMemory = 240 // 4 часа 1-минутных свечей
    RSLookbackMinutes  = 60  // 1 час для расчёта RS
    MinRSLookback      = 30  // Минимум 30 минут для старта анализа
    SupportLookback    = 60  // 60 минут для поиска поддержки
    VolumeAvgWindow    = 20  // 20 свечей для среднего объёма
    SignalCooldownMins = 15  // Cooldown между сигналами по символу
)
```

#### 4.2 **Data Storage — RingBuffer**

**Проблема старой реализации**: `slice[1:]` не освобождает память underlying array → memory leak.

**Решение**: Собственная реализация `RingBuffer`:

```go
type RingBuffer struct {
    data  []models.Candle
    size  int
    head  int
    count int
}

func (rb *RingBuffer) Push(c models.Candle) {
    rb.data[rb.head] = c
    rb.head = (rb.head + 1) % rb.size
    if rb.count < rb.size {
        rb.count++
    }
}
```

**Преимущества**:
- ✅ Фиксированный размер массива — нет аллокаций после инициализации
- ✅ O(1) операции Push/Get
- ✅ Нет memory leak

**Структура Engine**:
```go
type Engine struct {
    mu           sync.RWMutex
    candleCache  map[string]*RingBuffer // symbol → свечи
    btcBuffer    *RingBuffer            // BTC отдельно (не дублируется!)
    fundingRates map[string]float64     // symbol → funding rate (%)
    signalChan   chan *models.Signal
    lastSignal   map[string]time.Time   // symbol → время последнего сигнала
}
```

**Важно**: BTC хранится только в `btcBuffer`, не в `candleCache` — убрано дублирование.

#### 4.3 **Signal Cooldown**

**Проблема**: Если актив пробил поддержку и держится ниже неё 5 минут — отправляется 5 дубликатов.

**Решение**: 15-минутный cooldown между сигналами по одному символу:

```go
func (e *Engine) emitSignal(signal *models.Signal) {
    if lastTime, exists := e.lastSignal[signal.Symbol]; exists {
        if time.Since(lastTime).Minutes() < SignalCooldownMins {
            return // Пропускаем дубликат
        }
    }
    e.signalChan <- signal
    e.lastSignal[signal.Symbol] = time.Now()
}
```

#### 4.4 **Processing Pipeline**

```
┌─────────────────┐
│  New Candle     │
│  Received       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Validate       │  ← High >= Low? Close in range?
│  Candle         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Store in       │
│  RingBuffer     │  ← Memory-efficient circular buffer
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
│  Calculate RS   │  ← Relative Strength vs BTC (с защитой от div/0)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Funding Check  │  ← Crowded short filter
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Volume Check   │  ← Volume > 1.5x avg? (проверка до дорогих операций)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Wick Analysis  │  ← Close near bottom?
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Detect         │
│  Support Level  │  ← Fractal Low pattern (60 минут)
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
│  Calculate      │
│  Score (0-100)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Check          │
│  Cooldown       │  ← 15 минут между сигналами
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
6. **Не дублируют недавний сигнал** (cooldown 15 минут)

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

**Защита от Division by Zero**:
```go
func (e *Engine) calculateRS(symbol string) (float64, error) {
    // ...
    if assetOld == 0 {
        return 0, fmt.Errorf("asset old price is zero")
    }
    if btcOld == 0 {
        return 0, fmt.Errorf("BTC old price is zero")
    }
    // ...
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

**Период поиска**: Последние **60 минут** (было 30 — исправлено для достижимости бонуса возраста)

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

**Триггер**: Volume Ratio >= 1.5

**Примеры**:

| Avg Volume | Current Volume | Ratio | Результат |
|-----------|---------------|-------|-----------|
| 1000 | 2400 | 2.4x | ✅ Пробой подтверждён |
| 1000 | 1300 | 1.3x | ❌ Слабый объём |
| 1000 | 3500 | 3.5x | ✅ Очень сильный сигнал |

---

### 7. **Volatility Filter (v1.3.0)**

**Идея**: Отсекать "мёртвые" или стабильные активы, которые не имеют достаточной волатильности для прибыльной торговли.

**Формула — Normalized Daily Range (NDR)**:
```
NDR = (High24h - Low24h) / CurrentPrice
```

**Триггер**:
```
if NDR < 0.03 (3%)  →  сигнал отбрасывается
```

**Логика**: Активы с дневной волатильностью менее 3% не покроют комиссии и не дадут значимой прибыли.

**Пример**:
```
High24h: $100, Low24h: $95, CurrentPrice: $97
NDR = (100 - 95) / 97 = 5.15%  ✅ Актив достаточно волатилен

High24h: $50.5, Low24h: $49.5, CurrentPrice: $50
NDR = (50.5 - 49.5) / 50 = 2%  ❌ Слишком стабильный, пропускаем
```

**Обновление**: 24h статистика обновляется каждые 5 минут.

---

## Система скоринга

**Диапазон**: 0-100 баллов

### Факторы и веса

#### 1. **Слабость актива** (до 40 баллов)

```go
if rs < -3.0 {
    score += 30  // Базовая слабость
    if rs < -5.0 {
        score += 10  // Экстремальная слабость
    }
}
```

#### 2. **Объём пробоя** (до 30 баллов)

```go
if volumeRatio >= 1.5 {
    score += 10  // Базовый объём (NEW!)
    if volumeRatio >= 2.0 {
        score += 10  // Высокий объём
        if volumeRatio >= 3.0 {
            score += 10  // Исключительный объём
        }
    }
}
```

> **Исправлено**: Добавлен базовый score +10 для volume >= 1.5x (ранее диапазон 1.5x-2x не давал баллов).

#### 3. **Возраст уровня поддержки** (20 баллов)

```go
supportAgeMinutes := time.Since(support.Timestamp).Minutes()
if supportAgeMinutes > 20 {  // Было 30, исправлено
    score += 20
}
```

> **Исправлено**: Порог снижен до 20 минут и SupportLookback увеличен до 60 минут — бонус теперь достижим.

#### 4. **Дивергенция с BTC** (10 баллов)

```go
if btcChange > 0 && assetChange < 0 {
    score += 10  // BTC растёт, актив падает
}
```

#### 5. **Funding (положительный)** (20 баллов)

```go
if fundingRate > 0.01 {
    score += 20
}
```

#### 6. **Close Position (сильный пробой)** (10 баллов)

```go
if closePosition < 0.1 {
    score += 10
}
```

#### 7. **Pump Rollover (v1.3.0)** (15 баллов)

```go
// "Hangover" effect: актив вырос > 5% за 24h, но локально пробивает поддержку
// Это индикатор разворота памп-движения — трейдеры застряли в лонгах
if ticker24h.Price24hPcnt > 0.05 {  // > +5%
    score += 15
}
```

> **Логика**: Если актив значительно вырос за сутки (+5% и более), но сейчас пробивает локальную поддержку, это сильный сигнал на разворот — day-трейдеры "застряли" в длинных позициях.

---

### Итоговая таблица скоринга

| Фактор | Условие | Баллы |
|--------|---------|-------|
| **Слабость** | RS < -3% | +30 |
| | RS < -5% | +10 (бонус) |
| **Объём (Z-Score)** | Z-Score > 2.0 | +10 (v1.7.1) |
| | Z-Score > 2.5 | +10 |
| | Z-Score > 3.0 | +10 (бонус) |
| **Возраст уровня** | Age > 20 min | +20 |
| **Дивергенция** | BTC↑ Asset↓ | +10 |
| **Funding** | Funding > 0.01% | +20 |
| **Close Position** | Close < 10% свечи | +10 |
| **Pump Rollover** | *см. Smart/Legacy ниже* | +10/+20 |
| | | |
| **Максимум** | | **100 (cap)** |

> **Примечание (v1.7.1)**: Volume Ratio заменён на Volume Z-Score — статистически более точный метод, учитывающий дисперсию объёма.

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
| 50 | RS=-3.2%, Volume=1.7x, Level Age=25min |

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

#### Таблица `signals`

```sql
CREATE TABLE signals (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) REFERENCES assets(symbol),
    price_trigger DECIMAL NOT NULL,
    level_broken DECIMAL NOT NULL,
    breakdown_volume DECIMAL,
    score_rs DECIMAL,
    score_total INT,
    meta JSONB
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

---

## Обработка ошибок

### 1. **WebSocket Disconnection**

**Trigger**: Read error или timeout

**Action** (внутри WSClient — SRP):
```go
1. Логируем ошибку
2. Устанавливаем connected = false
3. Запускаем reconnect с exponential backoff
4. Delay: 1s → 2s → 4s → ... → 60s (cap)
5. Максимум 10 попыток
6. Если не удалось — отправляем ошибку в fatalChan (v1.7.0)
7. main.go получает ошибку и инициирует graceful shutdown
```

**v1.7.0 Graceful Shutdown:**
- ✅ БД соединения закрываются корректно
- ✅ Goroutines завершаются через context cancellation
- ✅ Финальные логи записываются
- ✅ Нет os.Exit(1) без cleanup

### 2. **Invalid Candle Data**

**Trigger**: Невалидные данные из WebSocket

**Action**:
```go
1. Валидация: High >= Low, Close in range, Volume >= 0
2. Логируем warning с деталями
3. Пропускаем свечу
4. Продолжаем обработку
```

### 3. **Database Errors**

**Trigger**: Connection lost, query failed

**Action**:
```go
1. Логируем ошибку с контекстом
2. Продолжаем работу (сигнал теряется, но бот не падает)
3. Connection pool автоматически восстановит соединение
```

### 4. **ByBit API 403 (Cloudflare)**

**Trigger**: REST API blocked by Cloudflare

**Action**:
```go
1. Пробуем альтернативные endpoints (api.bytick.com, api.bybit.nl)
2. Если все недоступны → используем fallback список символов
3. Для funding rates → используем нулевые значения
4. Продолжаем работу
```

### 5. **Division by Zero in RS**

**Trigger**: Old price = 0 (ошибка данных)

**Action**:
```go
1. calculateRS возвращает error
2. Сигнал не генерируется для этого символа
3. Логируем debug с причиной
```

---

## Производительность

### Метрики

| Метрика | Значение |
|---------|----------|
| **Memory** | ~50-100 MB (RingBuffer — без memory leak) |
| **CPU** | <5% (горутины + легковесные вычисления) |
| **Latency** | <100ms от получения свечи до генерации сигнала |
| **Throughput** | ~100 свечей/минуту (real-time) |

### Оптимизации

1. **RingBuffer**: Фиксированный размер, O(1) операции, без аллокаций
2. **BTC отдельно**: Нет дублирования в candleCache
3. **Early exit**: Volume check перед дорогими операциями
4. **Channels**: Буферизованные каналы (1000 элементов)
5. **Connection Pooling**: pgxpool с 2-10 коннектами
6. **Goroutines**: Отдельные горутины для I/O операций
7. **Signal Cooldown**: Предотвращение дубликатов без лишних вычислений

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

### Graceful Shutdown (v1.7.0)

```go
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

// v1.7.0: Слушаем ОБА канала — сигналы ОС и фатальные ошибки WebSocket
select {
case sig := <-sigChan:
    log.Info().Str("signal", sig.String()).Msg("🛑 Shutdown signal received")
case err := <-wsClient.GetFatalChan():
    log.Error().Err(err).Msg("🚨 Fatal WebSocket error, graceful shutdown")
}
cancel()
time.Sleep(2 * time.Second)
log.Info().Msg("👋 Bot stopped")
```

---

## Итоговая архитектура в цифрах

| Компонент | Технология | Описание |
|-----------|------------|----------|
| **Runtime** | Go 1.22 | Высокопроизводительный, низкий memory footprint |
| **Database** | PostgreSQL 15+ | Надёжное хранение сигналов |
| **WebSocket** | gorilla/websocket | Real-time стриминг свечей + auto-reconnect |
| **Logging** | zerolog | Structured JSON logs |
| **Deployment** | Railway | Containerized, auto-deploy |
| | | |
| **Latency** | <100ms | От свечи до сигнала |
| **Memory** | ~50-100MB | RingBuffer + CleanupInactiveSymbols (v1.7.0) |
| **Symbols** | 50 USDT Perps | Топ слабых по WeaknessScore |
| **Warmup** | 30 минут | До первого сигнала |
| **Cooldown** | 15 минут | Между сигналами по символу |
| **Weakness Scan** | ~2 сек | Worker Pool 5x параллельно (v1.7.0) |

---

## История изменений

### v1.8.0 (27 января 2026) — Anti-Bear-Trap Filters

**Цель:** Предотвращение входа в шорт на уже истощённых трендах ("bear trap" сценарии).

---

#### Изменения

**1. Oversold Filter (24h Fatigue)**

**Проблема:** Бот сигнализировал шорт на активах, которые уже упали на >15% за 24ч — высокий риск отскока.

**Решение:** Ранний фильтр на основе `Price24hPcnt`.

```go
// v1.8.0: Oversold Filter
if ticker24h.Price24hPcnt < -0.15 {  // -15%
    return nil  // BLOCK: asset already crashed, bear trap risk
}
```

**Логика:** Если актив упал более чем на 15% за сутки, он уже "oversold" — вероятность mean reversion выше, чем продолжение падения.

---

**2. Dynamic Support Age (Volatility Adjustment)**

**Проблема:** В периоды высокой волатильности молодые уровни поддержки (15 мин) — просто временные паузы, не настоящие уровни.

**Решение:** Динамический `minSupportAge` на основе `Change1h`.

```go
// v1.8.0: Dynamic Support Age
change1h := calculateChange1h(buffer)

minAge := 15.0  // Default
if change1h < -0.02 {  // Dropped > 2% in 1h = high volatility
    minAge = 45.0  // Require stronger/older support
}

if supportAge < minAge {
    return nil
}
```

| Условие | Min Support Age |
|---------|-----------------|
| Change1h >= -2% (нормально) | 15 минут |
| Change1h < -2% (высокая волатильность) | 45 минут |

---

**3. MA Extension Filter (Rubber Band Effect)**

**Проблема:** Шорт активов, слишком далеко ушедших от среднего — высокий риск mean reversion (эффект "резиновой ленты").

**Решение:** Фильтр на основе отклонения от SMA200.

```go
// v1.8.0: MA Extension Filter
sma200 := calculateSMA200(buffer)
deviation := (sma200 - currentPrice) / sma200

if deviation > 0.04 {  // Price > 4% below SMA200
    return nil  // BLOCK: extended, rubber band risk
}
```

**Логика:** Если цена более чем на 4% ниже SMA200, актив "перепродан" относительно среднего — вероятен отскок к среднему.

---

#### Новые константы

```go
const (
    // v1.8.0: Anti-Bear-Trap Filters
    OversoldThreshold24h       = -0.15 // -15% in 24h = oversold
    HighVolatilityThreshold1h  = -0.02 // -2% in 1h = high volatility
    HighVolatilitySupportAge   = 45.0  // 45 min support age during volatility
    DefaultSupportAge          = 15.0  // 15 min default
    SMAWindow                  = 200   // 200 candles for SMA
    MAExtensionThreshold       = 0.04  // 4% below SMA = extended
)
```

---

#### Новые методы в Engine

| Метод | Описание |
|-------|----------|
| `calculateSMA200(buffer)` | SMA из последних 200 свечей (allocation-free) |
| `calculateChange1h(buffer)` | Изменение цены за последний час |

---

#### Новые хелперы в utils/math.go

| Функция | Описание |
|---------|----------|
| `SMA(values)` | Simple Moving Average |
| `SMAFromPrices(prices, window)` | SMA с указанием окна |
| `CalculateDeviation(price, sma)` | % отклонения цены от SMA |

---

#### Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `internal/strategy/strategy.go` | 3 новых фильтра, 2 новых метода, 6 новых констант |
| `internal/utils/math.go` | 3 новых хелпер-функции |
| `cmd/bot/main.go` | Версия обновлена до v1.8.0 |
| `ARCHITECTURE.md` | Документация новых фильтров |

---

#### Порядок фильтров в Pipeline (fail fast)

```
1. Volatility Filter (v1.3.0) — NDR < 3% → BLOCK
2. Oversold Filter (v1.8.0) — Price24h < -15% → BLOCK  ← NEW
3. Smart Pump Filter (v1.5.0) — Price24h > 30% → BLOCK
4. MA Extension Filter (v1.8.0) — >4% below SMA200 → BLOCK  ← NEW
5. OI Divergence (v1.5.0) — Long Exit detected → BLOCK
6. RS Calculation — RS >= -3% → skip
7. Funding Check — Funding < -0.015% → skip
8. Volume Check — Volume < 1.5x → skip
9. Wick Check — Close > 30% of candle → skip
10. Support Detection — Dynamic Age (v1.8.0)  ← MODIFIED
11. Breakdown Confirmation
12. Score Calculation
```

---

### v1.7.1 (27 января 2026) — Volume Scoring Refactor

**Цель:** Устранение дублирования метрик объёма в системе скоринга.

---

#### Изменения

**1. Удалён Volume Ratio scoring**

Volume Ratio и Volume Z-Score измеряли одну и ту же метрику (аномальный объём) разными способами, что приводило к избыточному весу объёма в итоговом скоре (до 40 баллов).

**Было:**
```go
// Volume Ratio: до 30 баллов
if volumeRatio >= 1.5x → +10
if volumeRatio >= 2.0x → +10
if volumeRatio >= 3.0x → +10

// Volume Z-Score: +10 баллов
if volumeZScore > 3.0 → +10

// Итого: до 40 баллов за объём
```

**Стало:**
```go
// Volume Z-Score only: до 30 баллов
if volumeZScore > 2.0 → +10  // Moderate anomaly
if volumeZScore > 2.5 → +10  // Strong anomaly
if volumeZScore > 3.0 → +10  // Extreme anomaly
```

**2. Почему Z-Score лучше Ratio**

| Метрика | Формула | Недостаток |
|---------|---------|------------|
| **Ratio** | Current / Mean | Не учитывает дисперсию |
| **Z-Score** | (Current - Mean) / StdDev | ✅ Учитывает волатильность объёма |

Z-Score показывает, на сколько стандартных отклонений текущий объём отличается от среднего. Это статистически корректнее: если объём актива обычно сильно "гуляет" (высокий StdDev), то даже 2x не будет аномалией.

**3. Уточнена документация Pump Rollover**

Добавлено примечание, что Smart Pump Rollover (+20) и Legacy Pump Rollover (+10) — взаимоисключающие бонусы (else-if логика в коде).

---

#### Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `internal/strategy/strategy.go` | Удалён Volume Ratio scoring, расширена шкала Z-Score (2.0/2.5/3.0) |
| `ARCHITECTURE.md` | Обновлены таблицы скоринга, добавлены примечания |

---

### v1.7.0 (27 января 2026) — Performance & Stability Improvements

**Цель:** Устранение узких мест производительности, утечек памяти и улучшение graceful shutdown.

---

#### 1. Параллельное сканирование слабости (Worker Pool)

**Проблема:** Последовательные API вызовы с `time.Sleep(100ms)` — сканирование 100 символов занимало 10+ секунд.

**Решение:** Worker Pool с 5 параллельными воркерами.

**Новые константы:**
```go
const (
    scanWorkers      = 5                      // Параллельных воркеров
    apiRequestDelay  = 100 * time.Millisecond // Rate limiting per worker
    scanTimeout      = 5 * time.Minute        // Таймаут на весь скан
)
```

**Архитектура:**
```
┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│ symbolChan   │────▶│  Worker 1   │────▶│              │
│              │     ├─────────────┤     │  resultChan  │
│ (buffered)   │────▶│  Worker 2   │────▶│              │
│              │     ├─────────────┤     │  (buffered)  │
│              │────▶│  Worker 3   │────▶│              │
│              │     ├─────────────┤     │              │
│              │────▶│  Worker 4   │────▶│              │
│              │     ├─────────────┤     │              │
│              │────▶│  Worker 5   │────▶│              │
└──────────────┘     └─────────────┘     └──────────────┘
```

**Результат:**
- **Было:** ~10+ секунд (100 символов × 100ms)
- **Стало:** ~2 секунды (5 воркеров параллельно)
- **Ускорение:** ~5x

---

#### 2. Graceful Shutdown вместо log.Fatal()

**Проблема:** `log.Fatal()` при ошибке реконнекта WebSocket вызывал `os.Exit(1)` без корректного завершения (БД соединения, goroutines).

**Решение:** Канал для фатальных ошибок + graceful shutdown.

**Новые поля в WSClient:**
```go
type WSClient struct {
    // ...
    fatalChan  chan error // Канал для критических ошибок
}
```

**Новый метод:**
```go
func (c *WSClient) GetFatalChan() <-chan error
```

**Изменение в main.go:**
```go
// Было:
<-sigChan
log.Info().Msg("Shutdown signal received...")

// Стало:
select {
case sig := <-sigChan:
    log.Info().Str("signal", sig.String()).Msg("🛑 Shutdown signal received...")
case err := <-wsClient.GetFatalChan():
    log.Error().Err(err).Msg("🚨 Fatal WebSocket error, initiating graceful shutdown...")
}
```

**Результат:**
- ✅ Корректное закрытие БД соединений
- ✅ Graceful завершение goroutines
- ✅ Финальные логи записываются

---

#### 3. Устранение утечек памяти

**Проблема:** Карты `candleCache`, `lastSignal`, `oiSnapshots` росли бесконечно при ротации символов.

**Решение:** Метод очистки неактивных данных + исправление создания снапшотов.

**Новый метод в Engine:**
```go
// CleanupInactiveSymbols удаляет данные для символов, которые больше не отслеживаются
func (e *Engine) CleanupInactiveSymbols(activeSymbols map[string]struct{}) int
```

**Очищает:**
- `candleCache` — кэш свечей
- `lastSignal` — время последних сигналов  
- `oiSnapshots` — снапшоты Open Interest

**Исправление в calculateOIDivergence:**
```go
// v1.7.0 FIX: Only update snapshot if symbol is still actively tracked
if _, stillTracked := e.ticker24hStats[symbol]; stillTracked {
    e.oiSnapshots[symbol] = &models.OISnapshot{...}
}
```

**Расширен GetStats():**
```go
return map[string]interface{}{
    "tracked_symbols": len(e.candleCache),
    "btc_candles":     e.btcBuffer.Len(),
    "oi_snapshots":    len(e.oiSnapshots),    // NEW
    "last_signals":    len(e.lastSignal),     // NEW
}
```

**Интеграция:**
- Вызывается в `updateWeaknessScores()` после `wsClient.CleanupInactive()`
- BTCUSDT никогда не удаляется

---

#### 4. Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `internal/strategy/weakness_scanner.go` | Worker Pool, параллельная обработка, таймаут |
| `internal/strategy/strategy.go` | `CleanupInactiveSymbols()`, расширенный `GetStats()`, fix в `calculateOIDivergence()` |
| `internal/bybit/websocket.go` | `fatalChan`, `GetFatalChan()`, graceful error handling |
| `cmd/bot/main.go` | Обработка `fatalChan`, вызов `CleanupInactiveSymbols()` |

---

### v1.6.0 (27 января 2026) — Dynamic WebSocket Subscriptions

**Проблема:** WebSocket подписки фиксировались при старте бота и не обновлялись при пересчёте weakness scores. Новые слабые активы не попадали в мониторинг до перезапуска.

**Решение:** Динамическое добавление/удаление WebSocket подписок.

**Новые методы WSClient:**
```go
// Подписка на новые символы (игнорирует уже подписанные)
func (c *WSClient) AddSubscriptions(symbols []string) error

// Количество активных подписок
func (c *WSClient) GetSubscribedCount() int

// Отписка от неактивных символов (не были в топ слабых > maxInactiveTime)
func (c *WSClient) CleanupInactive(maxInactiveTime time.Duration) int
```

**Новые структуры данных в WSClient:**
```go
subscribedSymbols map[string]struct{}  // Set подписанных символов
lastActive        map[string]time.Time // Время последнего появления в топ слабых
subMu             sync.RWMutex         // Mutex для thread-safety
```

**Логика работы:**
1. При обновлении weakness scores (каждые 15 мин) вызывается `AddSubscriptions()`
2. Новые символы добавляются в подписки, `lastActive` обновляется для всех
3. `CleanupInactive(2h)` отписывает символы, не появлявшиеся в топе > 2 часов
4. BTCUSDT никогда не удаляется (нужен для RS)
5. При reconnect подписываемся на ВСЕ символы из `subscribedSymbols`

**Константы:**
```go
MaxInactiveTime      = 2 * time.Hour  // Время до отписки неактивного символа
maxSubscriptionsWarn = 200            // Soft limit для предупреждения
```

**Преимущества:**
- ✅ Новые слабые активы сразу попадают в мониторинг
- ✅ RingBuffer сохраняется при временном выходе из топа
- ✅ Предотвращение неограниченного роста подписок
- ✅ Thread-safe операции
- ✅ Корректный reconnect со всеми подписками

**Критические исправления (v1.6.0 bugfix):**

1. **Race Condition Fix**: Добавлен отдельный `writeMu sync.Mutex` для всех операций записи в WebSocket. Gorilla WebSocket не потокобезопасен для записи — одновременные ping, subscribe и unsubscribe вызывали data corruption.

2. **Dynamic Ticker Data**: `updateTickerData()` теперь использует `wsClient.GetSubscribedSymbols()` вместо статического списка. Ранее новые символы не получали funding rates и 24h stats.

3. **Rollback on Error**: `AddSubscriptions()` теперь откатывает добавленные символы из map при ошибке подписки, предотвращая inconsistent state.

4. **Новый метод `GetSubscribedSymbols()`**: Возвращает копию списка подписанных символов для использования в других компонентах.

---

### v1.5.1 (26 января 2026) — Faster Weakness Scanner

**Проблема:** WeaknessScanner обновлялся раз в час. В крипте за час монета может упасть на 20% и отскочить — слишком медленно.

**Решение:** Интервал уменьшен с 1 часа до 15 минут.

```go
// Было:
weaknessTicker := time.NewTicker(1 * time.Hour)

// Стало:
weaknessTicker := time.NewTicker(15 * time.Minute)
```

**Влияние на API:**
- 1 запрос каждые 15 минут — минимальная нагрузка
- Список "жертв" всегда актуален
- Быстрее реагируем на изменения рынка

---

### v1.5.0 (26 января 2026) — Smart Filters & OI Divergence

**Цель:** Внедрение микроструктурных фильтров для повышения WinRate.

**1. Open Interest Divergence Analysis**

Новая система анализа открытого интереса для фильтрации ложных сигналов:

| Сценарий | Условие | Действие |
|----------|---------|----------|
| **Aggressive Short** | Price ↓ + OI ↑ (>2%) | +15 баллов (новые шорты входят) |
| **Long Exit** | Price ↓ + OI ↓ (>2%) | **BLOCK** (лонги выходят, не шорт-сигнал) |

**Реализация:**
- ✅ `OpenInterest` добавлен в `Ticker24hStats` (models.go)
- ✅ `OISnapshot` — структура для хранения исторических значений OI
- ✅ `oiSnapshots` map в Engine для отслеживания OI 15 минут назад
- ✅ `calculateOIDivergence()` — расчёт deltaOI и определение типа движения

**Константы:**
```go
OISnapshotAgeMinutes  = 15.0   // Возраст snapshot для delta
OIDivergenceThreshold = 0.02  // 2% порог изменения OI
OIAggressiveShortBonus = 15   // Бонус за aggressive short
OILongExitPenalty     = -50   // Блокировка при long exit
```

**2. Volume Z-Score Analysis**

Замена грубого Volume Ratio на статистически корректный Z-Score:

| Z-Score | Интерпретация | Действие |
|---------|---------------|----------|
| > 3.0 | Сильная аномалия | +10 баллов |
| 2.0-3.0 | Умеренная аномалия | Используется ratio scoring |
| < 2.0 | Нормальный объём | Используется ratio scoring |

**Реализация:**
- ✅ `internal/utils/math.go` — пакет математических функций (StdDev, Mean, ZScore)
- ✅ `CalculateVolumeZScore()` — расчёт Z-Score для объёма
- ✅ `calculateVolumeZScore()` в Engine — использует последние 24 свечи

**Формула:**
```
Z-Score = (CurrentVolume - Mean) / StdDev
```

**3. Smart Pump Filter**

Замена жёсткого фильтра `MaxPriceGain24h = 10%` на интеллектуальную систему:

| Условие | Действие |
|---------|----------|
| Price24hPcnt > 30% | **BLOCK** (слишком сильный памп) |
| Price24hPcnt > 15% && DistFromHigh < 3% | **BLOCK** (ловля падающего ножа) |
| Price24hPcnt > 15% && DistFromHigh > 5% && Z-Score > 3.0 | +20 баллов (подтверждённый разворот пампа) |

**Новые константы:**
```go
MaxPriceGain24h       = 0.30  // Увеличено с 0.10 до 0.30 (30%)
SmartPumpThreshold    = 0.15  // 15% триггер smart filter
SmartPumpNearHighDist = 0.03  // 3% от хая = ещё близко (BLOCK)
SmartPumpRolloverDist = 0.05  // 5% от хая = подтверждённый откат
SmartPumpRolloverBonus = 20   // Бонус за smart rollover
```

**Логика DistanceFromHigh:**
```go
DistFromHigh = (High24h - CurrentPrice) / High24h
```

**4. Обновлённая таблица скоринга**

| Фактор | Условие | Баллы |
|--------|---------|-------|
| **Слабость** | RS < -3% | +30 |
| | RS < -5% | +10 (бонус) |
| **Объём (Z-Score)** | Z-Score > 2.0 | +10 (v1.7.1) |
| | Z-Score > 2.5 | +10 |
| | Z-Score > 3.0 | +10 (бонус) |
| **Возраст уровня** | Age > 20 min | +20 |
| **Дивергенция с BTC** | BTC↑ Asset↓ | +10 |
| **Funding** | Funding > 0.01% | +20 |
| **Close Position** | Close < 10% свечи | +10 |
| **OI Divergence** | Price↓ + OI↑ (>2%) | +15 (v1.5.0) |
| **Pump Rollover** ¹ | Smart: Pump>15%, Dist>5%, Z>3 | +20 |
| | Legacy: Pump>5%, Red candle | +10 |
| **Support Touches** | >= 3 касания | +15 |
| | >= 2 касания | +10 |
| **Consolidation Break** | IsConsolidation=true | +10 |
| | | |
| **Максимум** | | **100 (cap)** |

> ¹ **Pump Rollover (v1.5.0)**: Smart и Legacy бонусы **взаимоисключающие** (else-if). Если сигнал квалифицируется как Smart Pump Rollover (+20), legacy логика пропускается. Максимум один из двух бонусов.

> **v1.7.1**: Volume Ratio удалён — теперь используется только Volume Z-Score для избежания дублирования метрик.

**5. Новые поля в Signal.Meta**

```go
Meta: map[string]interface{}{
    // Existing fields...
    
    // v1.5.0 metrics
    "volume_z_score":         float64,  // Z-Score объёма
    "volume_mean":            float64,  // Средний объём (для отладки)
    "volume_stddev":          float64,  // StdDev объёма (для отладки)
    "oi_delta_pct":           float64,  // Изменение OI в %
    "is_aggressive_short":    bool,     // Price↓ + OI↑
    "is_smart_pump_rollover": bool,     // Подтверждённый разворот пампа
    "dist_from_high_pct":     float64,  // Расстояние от хая в %
}
```

**6. Новые файлы**

- `internal/utils/math.go` — математические функции (StdDev, Mean, ZScore, DistanceFromHigh)

**7. Изменения в существующих файлах**

| Файл | Изменения |
|------|-----------|
| `internal/models/models.go` | Добавлен `OpenInterest` в `Ticker24hStats`, новая структура `OISnapshot` |
| `internal/bybit/api.go` | Парсинг `openInterest` из API response |
| `internal/strategy/strategy.go` | Новые константы, `oiSnapshots` map, методы `calculateOIDivergence()`, `calculateVolumeZScore()`, `calculateScoreV150()` |

**8. Архитектурные принципы**

- **DRY**: Математические функции вынесены в отдельный пакет `utils`
- **SOLID/SRP**: Каждая функция отвечает за один аспект анализа
- **Early Exit**: OI Long Exit блокирует сигнал до дорогих операций
- **Backwards Compatibility**: Сохранена legacy scoring функция


---

### v1.4.0 (26 января 2026)

**ФАЗА 1: Система отбора слабых активов**

Полностью переделана логика выбора инструментов для мониторинга:
- ✅ **WeaknessScore** — новая структура с метриками слабости актива (RS7d, RS24h, MA50, MA200, VolumeDecline)
- ✅ **WeaknessScorer** — расчёт скора слабости (0-100) на основе 6 факторов
- ✅ **WeaknessScanner** — сканер для выбора топ-50 слабых активов
- ✅ **GetKlines()** — получение исторических свечей (D, 4h) для расчёта MA и RS
- ✅ **GetAllInstruments()** — получение всех инструментов с минимальным turnover $5M

**Критерии WeaknessScore:**
| Фактор | Условие | Баллы |
|--------|---------|-------|
| RS7d | < -20% | +30 |
| RS7d | < -10% | +20 |
| RS7d | < -5% | +10 |
| RS24h | < -10% | +15 |
| RS24h | < -5% | +10 |
| RS24h | < -2% | +5 |
| PriceVsMA50 | < -20% | +20 |
| PriceVsMA50 | < -10% | +15 |
| PriceVsMA50 | < -5% | +10 |
| PriceVsMA200 | < -30% | +15 |
| PriceVsMA200 | < -15% | +10 |
| VolumeDecline | < -30% | +10 |
| VolumeDecline | < -15% | +5 |
| FundingRate | > 0.05% | +10 |
| FundingRate | > 0.01% | +5 |

**ФАЗА 2: Улучшение Support Detection**

- ✅ **DetectConsolidation()** — детекция зон консолидации (range < 3%, 60 свечей)
- ✅ **DetectFractalLow()** — fallback на фрактальные минимумы
- ✅ **ConfirmNoBounceback()** — проверка buyback wick текущей свечи пробоя
- ✅ **TouchCount** — подсчёт касаний уровня поддержки
- ✅ **IsConsolidation** — флаг пробоя консолидации (сильнее фрактала)

**Исправленные баги и оптимизации:**
- ✅ **weakness_scanner.go**: Исправлен возврат `nil, nil` → корректная ошибка
- ✅ **weakness_scanner.go**: Добавлен rate limiting (100ms между API запросами)
- ✅ **breakdown_analyzer.go**: Исправлена логика ConfirmNoBounceback (проверка wick текущей свечи)
- ✅ **support_detector.go**: Обработка edge case с нулевыми ценами в findHighLow()
- ✅ **main.go**: Защита от nil pointer dereference в updateTickerData()

**Новые бонусы скоринга:**
| Фактор | Условие | Баллы |
|--------|---------|-------|
| Support Touches | >= 3 касания | +15 |
| Support Touches | >= 2 касания | +10 |
| Consolidation Break | IsConsolidation=true | +10 |

**Новые файлы:**
- `internal/models/weakness.go` — WeaknessScore, HistoricalCandle
- `internal/strategy/weakness_scorer.go` — расчёт скора слабости
- `internal/strategy/weakness_scanner.go` — сканер слабых активов
- `internal/strategy/support_detector.go` — детектор консолидации
- `internal/strategy/breakdown_analyzer.go` — проверка bounceback

**Изменения в архитектуре:**
- Отбор символов: mid-cap по объёму → топ-50 слабых по WeaknessScore
- Периодическое обновление WeaknessScore каждые 15 минут (v1.5.1)
- Support detection с приоритетом консолидации

### v1.4.1 (26 января 2026) — Hotfix: Anti-False-Signal Filters

**Проблема:** Ложные сигналы на FHEUSDT (+21% за 24ч) и MERLUSDT с молодыми уровнями (6-9 мин).

**Исправления:**

| Фильтр | Описание | Порог |
|--------|----------|-------|
| **Min Support Age** | Уровни младше 15 минут отклоняются | 15 мин |
| **Max Price Gain 24h** | Активы с ростом >10% за 24ч не шортятся | 10% |
| **Pump Rollover Fix** | Бонус только если текущая свеча КРАСНАЯ | Close < Open |

**Новые константы:**
```go
MinSupportAgeMinutes = 15.0  // Минимальный возраст уровня
MaxPriceGain24h      = 0.10  // Макс. рост за 24ч для шорта
```

**Логика Pump Rollover (исправлена):**
- Раньше: бонус за любой рост >5%
- Теперь: бонус только если рост >5% И текущая свеча красная (подтверждение разворота)

---

### v1.3.0 (26 января 2026)

**Новые фильтры и бонусы**:
- ✅ **Volatility Filter**: Отсечение "мёртвых" активов с NDR < 3%
- ✅ **Pump Rollover Bonus**: +15 баллов если 24h Change > +5% (разворот пампа)

**Новый функционал**:
- ✅ `GetTickerData()` — объединённый метод для получения funding rates + 24h статистики в **одном API запросе**
- ✅ `UpdateTicker24hStats()` — периодическое обновление 24h данных в Engine
- ✅ Структура `Ticker24hStats` и `TickerData` в models/bybit

**Изменения в Pipeline**:
- Volatility Filter выполняется в начале analyzeBreakdown() (early exit для мёртвых активов)
- Pump Rollover Bonus добавлен в calculateScore()
- Debug логирование при отклонении по volatility filter

**Оптимизации производительности**:
- ✅ **Single API Call**: funding rates и 24h stats теперь получаются одним HTTP запросом вместо двух
- ✅ Убраны дублирующиеся тикеры в runPeriodicTasks()

---

### v1.2.0 (26 января 2026)

**Исправленные баги**:
- ✅ WebSocket reconnect перенесён внутрь WSClient (SRP)
- ✅ RingBuffer вместо slice trimming (memory leak fix)
- ✅ Division by zero protection в calculateRS
- ✅ Candle validation перед обработкой
- ✅ SupportLookback увеличен до 60 минут
- ✅ Support age bonus порог снижен до 20 минут
- ✅ Volume scoring: добавлен базовый score для 1.5x+
- ✅ BTC candles не дублируются в candleCache
- ✅ Signal cooldown 15 минут

**Принципы**:
- **SRP**: WSClient управляет своим соединением
- **DRY**: RingBuffer переиспользуется для всех символов
- **Early Exit**: Дешёвые проверки перед дорогими операциями

### v1.1.0 (ранее)

- Базовая реализация стратегии
- WebSocket стриминг
- Telegram уведомления

---

**Документация актуальна на**: 27 января 2026
**Версия бота**: 1.8.0
**Автор архитектуры**: AI-assisted development
