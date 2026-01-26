# v1.4.0

## Контекст
Бот детектирует breakdown сигналы для шорта криптовалют. Текущая проблема: отбираются mid-cap активы по объёму торгов вместо фундаментально слабых активов. Нужно переделать систему отбора и улучшить детекцию пробоя.

---

## ФАЗА 1: Система отбора слабых активов

### 1.1 Новая структура WeaknessScore

**Файл:** `internal/models/weakness.go`

```go
type WeaknessScore struct {
    Symbol          string
    RS7d            float64 // Relative Strength vs BTC за 7 дней (%)
    RS24h           float64 // Relative Strength vs BTC за 24 часа (%)
    PriceVsMA50     float64 // (Price - MA50) / MA50 * 100 (%)
    PriceVsMA200    float64 // (Price - MA200) / MA200 * 100 (%)
    VolumeDecline   float64 // (AvgVol7d - AvgVol30d) / AvgVol30d * 100 (%)
    FundingRate     float64 // Текущий funding rate (%)
    TotalScore      float64 // Итоговый скор слабости (0-100)
    UpdatedAt       time.Time
}
```

### 1.2 Расчёт WeaknessScore

**Файл:** `internal/strategy/weakness_scorer.go`

**Формула TotalScore:**
```
score = 0

// RS7d: чем ниже, тем слабее актив
if RS7d < -20%: score += 30
else if RS7d < -10%: score += 20
else if RS7d < -5%: score += 10

// RS24h: краткосрочная слабость
if RS24h < -10%: score += 15
else if RS24h < -5%: score += 10
else if RS24h < -2%: score += 5

// Цена ниже MA50
if PriceVsMA50 < -20%: score += 20
else if PriceVsMA50 < -10%: score += 15
else if PriceVsMA50 < -5%: score += 10

// Цена ниже MA200
if PriceVsMA200 < -30%: score += 15
else if PriceVsMA200 < -15%: score += 10

// Падающий объём (актив теряет интерес)
if VolumeDecline < -30%: score += 10
else if VolumeDecline < -15%: score += 5

// Funding положительный (лонгисты платят — потенциал падения)
if FundingRate > 0.05%: score += 10
else if FundingRate > 0.01%: score += 5

TotalScore = min(score, 100)
```

### 1.3 Получение исторических данных

**Файл:** `internal/bybit/api.go`

Добавить методы:

```go
// GetKlines — получить исторические свечи для расчёта MA и RS
// GET /v5/market/kline
// Параметры: symbol, interval ("D" для дневных), limit (200 для MA200)
func (c *Client) GetKlines(symbol string, interval string, limit int) ([]Candle, error)

// Интервалы для использования:
// - "D" (daily) — для MA50, MA200, RS7d
// - "240" (4h) — для RS24h если нужна точность
```

**Расчёт MA:**
```go
func calculateMA(candles []Candle, period int) float64 {
    if len(candles) < period { return 0 }
    sum := 0.0
    for i := len(candles) - period; i < len(candles); i++ {
        sum += candles[i].Close
    }
    return sum / float64(period)
}
```

**Расчёт RS (7d):**
```go
func calculateRS7d(assetCandles, btcCandles []Candle) float64 {
    // assetCandles и btcCandles — дневные свечи
    if len(assetCandles) < 7 || len(btcCandles) < 7 { return 0 }
    
    assetChange := (assetCandles[len(assetCandles)-1].Close - assetCandles[len(assetCandles)-7].Close) / 
                    assetCandles[len(assetCandles)-7].Close * 100
    
    btcChange := (btcCandles[len(btcCandles)-1].Close - btcCandles[len(btcCandles)-7].Close) / 
                  btcCandles[len(btcCandles)-7].Close * 100
    
    return assetChange - btcChange
}
```

### 1.4 WeaknessScanner

**Файл:** `internal/strategy/weakness_scanner.go`

```go
type WeaknessScanner struct {
    client       *bybit.Client
    scores       map[string]*WeaknessScore // symbol → score
    mu           sync.RWMutex
    btcCandles   []Candle // Кэш BTC дневных свечей
}

// ScanAll — пересчитать WeaknessScore для всех активов
// Вызывать раз в 1 час
func (ws *WeaknessScanner) ScanAll(symbols []string) error

// GetTopWeak — вернуть топ-N слабых активов
func (ws *WeaknessScanner) GetTopWeak(n int) []WeaknessScore

// GetScore — получить скор для символа
func (ws *WeaknessScanner) GetScore(symbol string) *WeaknessScore
```

### 1.5 Изменение логики отбора символов

**Файл:** `cmd/bot/main.go`

**Было:**
```go
symbols := api.GetInstruments() // Top 16-116 по turnover
```

**Стало:**
```go
// 1. Получить ВСЕ USDT Perp символы с минимальной ликвидностью
allSymbols := api.GetAllInstruments(minTurnover: 5_000_000) // $5M/24h минимум

// 2. Рассчитать WeaknessScore для всех
weaknessScanner.ScanAll(allSymbols)

// 3. Взять топ-50 слабых для мониторинга
weakSymbols := weaknessScanner.GetTopWeak(50)

// 4. Подписаться на WebSocket только для слабых
wsClient.Subscribe(weakSymbols)
```

### 1.6 Периодическое обновление

**Добавить в `runPeriodicTasks`:**
```go
// Каждый час пересчитывать WeaknessScore и обновлять список символов
weaknessTicker := time.NewTicker(1 * time.Hour)
go func() {
    for range weaknessTicker.C {
        weaknessScanner.ScanAll(allSymbols)
        newWeakSymbols := weaknessScanner.GetTopWeak(50)
        wsClient.UpdateSubscriptions(newWeakSymbols)
    }
}()
```

---

## ФАЗА 2: Улучшение Support Detection

### 2.1 Новая структура SupportLevel

**Файл:** `internal/models/support.go`

```go
type SupportLevel struct {
    Price           float64
    FormationTime   time.Time   // Когда уровень сформировался
    TouchCount      int         // Сколько раз цена касалась уровня
    VolumeAtLevel   float64     // Средний объём при касаниях
    RangeHigh       float64     // Верхняя граница консолидации
    RangeLow        float64     // Нижняя граница (= Price)
    IsConsolidation bool        // Был ли период консолидации
}
```

### 2.2 Детектор консолидации

**Файл:** `internal/strategy/support_detector.go`

```go
const (
    ConsolidationLookback = 60    // Искать консолидацию за 60 свечей
    ConsolidationMinBars  = 20    // Минимум 20 свечей в консолидации
    ConsolidationMaxRange = 0.03  // Максимальный диапазон 3% для консолидации
    TouchThreshold        = 0.003 // 0.3% — допуск для "касания" уровня
)

// DetectConsolidation — найти зону консолидации
func DetectConsolidation(candles []Candle) *SupportLevel {
    if len(candles) < ConsolidationLookback { return nil }
    
    recent := candles[len(candles)-ConsolidationLookback:]
    
    // Найти High и Low за период
    high, low := findHighLow(recent)
    avgPrice := (high + low) / 2
    rangePercent := (high - low) / avgPrice
    
    // Если диапазон > 3% — не консолидация
    if rangePercent > ConsolidationMaxRange { return nil }
    
    // Посчитать касания нижней границы
    touchCount := 0
    for _, c := range recent {
        if math.Abs(c.Low - low) / low < TouchThreshold {
            touchCount++
        }
    }
    
    return &SupportLevel{
        Price:           low,
        FormationTime:   recent[0].Timestamp,
        TouchCount:      touchCount,
        RangeHigh:       high,
        RangeLow:        low,
        IsConsolidation: true,
    }
}
```

### 2.3 Fallback на Fractal Low

Если консолидация не найдена, использовать текущий Fractal Low как fallback:

```go
func DetectSupport(candles []Candle) *SupportLevel {
    // Приоритет 1: Консолидация
    if support := DetectConsolidation(candles); support != nil {
        return support
    }
    
    // Приоритет 2: Fractal Low (текущая логика)
    return DetectFractalLow(candles)
}
```

### 2.4 Проверка отсутствия откупа (Bounceback Check)

**Файл:** `internal/strategy/breakdown_analyzer.go`

```go
const (
    BouncebackCheckBars = 2  // Проверяем 2 свечи после пробоя
    MaxBouncePercent    = 0.5 // Bounce не должен превышать 50% падения
)

// ConfirmNoBounceback — проверить, что после пробоя нет сильного откупа
// Вызывать ПОСЛЕ детекции пробоя, перед генерацией сигнала
func ConfirmNoBounceback(candles []Candle, support float64) bool {
    if len(candles) < BouncebackCheckBars + 1 { return true }
    
    // Свеча пробоя
    breakdownCandle := candles[len(candles) - BouncebackCheckBars - 1]
    
    // Глубина падения
    dropDepth := support - breakdownCandle.Close
    if dropDepth <= 0 { return false }
    
    // Проверяем последующие свечи
    for i := len(candles) - BouncebackCheckBars; i < len(candles); i++ {
        c := candles[i]
        
        // Если закрылись выше support — откуп произошёл
        if c.Close > support {
            return false
        }
        
        // Если bounce > 50% от падения — слишком сильный откуп
        bounceFromLow := c.Close - breakdownCandle.Low
        if bounceFromLow > dropDepth * MaxBouncePercent {
            return false
        }
    }
    
    return true
}
```

### 2.5 Обновление analyzeBreakdown

**Файл:** `internal/strategy/engine.go`

Изменить `analyzeBreakdown`:

```go
func (e *Engine) analyzeBreakdown(symbol string, candle Candle) *Signal {
    // ... существующие проверки (RS, Funding, Volatility) ...
    
    // Детекция поддержки (НОВОЕ)
    support := DetectSupport(candles)
    if support == nil { return nil }
    
    // Проверка пробоя
    if candle.Close >= support.Price { return nil }
    
    // НОВОЕ: Проверка отсутствия откупа
    if !ConfirmNoBounceback(candles, support.Price) {
        log.Debug().Str("symbol", symbol).Msg("Bounceback detected, skipping signal")
        return nil
    }
    
    // ... остальная логика (volume, wick check, score) ...
    
    // НОВОЕ: Добавить данные о support в сигнал
    signal.SupportTouchCount = support.TouchCount
    signal.IsConsolidationBreak = support.IsConsolidation
    
    return signal
}
```

### 2.6 Обновление скоринга

Добавить бонусы за качество support:

```go
// В calculateScore():

// Бонус за количество касаний уровня
if support.TouchCount >= 3 {
    score += 15  // Многократно тестированный уровень
} else if support.TouchCount >= 2 {
    score += 10
}

// Бонус за пробой консолидации
if support.IsConsolidation {
    score += 10  // Пробой консолидации сильнее, чем пробой fractal low
}
```

---

## Итоговые изменения в файлах

| Файл | Действие |
|------|----------|
| `internal/models/weakness.go` | CREATE — структура WeaknessScore |
| `internal/models/support.go` | UPDATE — новая структура SupportLevel |
| `internal/bybit/api.go` | UPDATE — добавить GetKlines(), GetAllInstruments() |
| `internal/strategy/weakness_scorer.go` | CREATE — расчёт WeaknessScore |
| `internal/strategy/weakness_scanner.go` | CREATE — сканер слабых активов |
| `internal/strategy/support_detector.go` | CREATE — детектор консолидации |
| `internal/strategy/breakdown_analyzer.go` | CREATE — проверка bounceback |
| `internal/strategy/engine.go` | UPDATE — интеграция новой логики |
| `cmd/bot/main.go` | UPDATE — новая логика отбора символов |

---

## API Endpoints (Bybit)

**Для WeaknessScore:**
```
GET /v5/market/kline
  ?category=linear
  &symbol=BTCUSDT
  &interval=D
  &limit=200

Response: {result: {list: [[timestamp, open, high, low, close, volume, turnover], ...]}}
```

**Для списка всех инструментов:**
```
GET /v5/market/tickers
  ?category=linear

Response: {result: {list: [{symbol, turnover24h, ...}, ...]}}
```

---

## Порядок реализации

1. `internal/models/weakness.go` — структуры
2. `internal/bybit/api.go` — GetKlines, GetAllInstruments
3. `internal/strategy/weakness_scorer.go` — расчёт скора
4. `internal/strategy/weakness_scanner.go` — сканер
5. `internal/strategy/support_detector.go` — детектор консолидации
6. `internal/strategy/breakdown_analyzer.go` — bounceback check
7. `internal/strategy/engine.go` — интеграция
8. `cmd/bot/main.go` — изменение точки входа
9. Тестирование