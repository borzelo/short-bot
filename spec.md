
# Technical Specification: Weak Asset Breakdown Bot (Go + PostgreSQL)

## 1. Project Context

**Objective:** Build a high-performance backend service in **Go** that monitors the crypto futures market (ByBit USDT Perps) to detect "Weak Asset Breakdown" setups.
**Strategy Logic:**

1. **Identify Weakness:** Find assets that are underperforming BTC.
2. **Locate Support:** Identify key local support levels on the 1m timeframe.
3. **Signal Breakdown:** Trigger an alert when price breaks support with high volume.
4. **Scoring:** Assign a confidence score (0-100) to each signal.

## 2. Architecture & Tech Stack

### 2.1 Core Stack

* **Language:** Go (Golang) 1.22+
* **Database:** PostgreSQL 15+ (pgx driver).
* **Exchange:** ByBit V5 API (Unified Trading Account).
* **Messaging:** Telegram Bot API.

### 2.2 Hybrid Approach

* **Go:** Handles the "Hot Path" (Websockets, real-time calculation).
* **Python:** (Reserved for future) Offline data analysis.

## 3. Data Flow & Modules

### Module A: Market Data Streamer (`pkg/market`)

* **Input:** ByBit WebSocket `public/linear` topic `kline.1` (1-minute candles).
* **State:** Thread-safe cache `map[Symbol]RingBuffer`.
* **Universe:** Top 100 USDT Futures by 24h Volume.

### Module B: Screener Engine (`pkg/strategy`)

* **Trigger:** Executed on every closed 1m candle.
* **Logic Flow:**
1. **RS Calculation:** `(Asset_%_Change - BTC_%_Change)` over 4h.
2. **Support Detection:** Fractal Low pattern (Low[i] < Low[i-2...i+2]) within last 60m.
3. **Breakdown Trigger:** `Close < Support` AND `Volume > 1.5x Avg`.
4. **Scoring Calculator (0-100):**
* *Base:* 0
* *Weakness:* If RS < -3% (+30 pts), If RS < -5% (+10 pts extra).
* *Volume:* If Vol > 2x (+20 pts), If Vol > 3x (+10 pts extra).
* *Level Age:* If Support existed > 30 mins (+20 pts).
* *Trend:* If BTC is Green (>0%) but Coin is Red (+10 pts).
* *Max:* 100.





### Module C: Persistence (`pkg/storage`)

* **Task:** Save the generated signal with `total_score` to PostgreSQL.

### Module D: Notifier (`pkg/telegram`)

* **Task:** Send a structured alert in **Russian language**.

## 4. Database Schema (PostgreSQL)

```sql
CREATE TABLE IF NOT EXISTS assets (
    symbol VARCHAR(20) PRIMARY KEY,
    tick_size DECIMAL NOT NULL,
    min_lot_size DECIMAL NOT NULL,
    is_tradable BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) REFERENCES assets(symbol),
    
    -- Technical Data
    price_trigger DECIMAL NOT NULL,
    level_broken DECIMAL NOT NULL,
    breakdown_volume DECIMAL,
    
    -- Scoring Data
    score_rs DECIMAL,       -- Relative Strength
    score_total INT,        -- Final Score 0-100
    
    meta JSONB              -- Extra data (RSI, ATR, etc.)
);

CREATE INDEX idx_signals_symbol_time ON signals(symbol, created_at DESC);

```

## 5. Implementation Prompt for Agent

**Context:**
Create a Go trading bot detection engine.

**Instructions:**

1. **Structure:** `cmd/bot`, `internal/strategy`, `internal/bybit`, `internal/db`.
2. **Scoring Logic:** Implement a `CalculateScore()` function inside `internal/strategy` that aggregates factors (RS, Volume, Support Duration) into an `int` (0-100).
3. **Localization:** The Telegram message template must be in **Russian**.

**Required Telegram Message Format (Russian):**

```text
🚨 **СИГНАЛ: ПРОБОЙ УРОВНЯ** 🚨

Тикер: #SOLUSDT
Цена: 142.10 (Пробит уровень: 142.50)

📉 **Факторы слабости:**
• Относит. сила (RS): -4.2% (Слабее рынка)
• Объем при пробое: x2.4 от среднего

🛠 **Технический анализ:**
• RSI: 38
• Время жизни уровня: 45 мин

🏆 **ОБЩИЙ БАЛЛ: 85/100**
*(Высокая вероятность падения)*

```