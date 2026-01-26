
# SPECIFICATION: Strategy Logic Upgrade (v1.3.0)

**Context:** Upgrade "Millionaire Bot" from v1.2 to v1.3.
**Goal:** Implement "Pump Rollover" logic (shorting assets that are green on the daily timeframe but breaking down locally) and a "Volatility Filter" to avoid dead assets.

## 1. Module: Market Data (`internal/bybit`)

### 1.1 Ensure Ticker Data Availability

**File:** `internal/bybit/api.go` / `models/ticker.go`
**Action:** Verify that the `Ticker` struct and the API response parsing include the following fields from ByBit V5 `GET /v5/market/tickers`:

* `highPrice24h` (High Price 24h)
* `lowPrice24h` (Low Price 24h)
* `price24hPcnt` (24h Price Change %)

**Constraint:** Ensure these fields are passed to the `Strategy Engine` during the periodic update loop.

---

## 2. Module: Strategy Engine (`internal/strategy`)

### 2.1 Filter: Volatility Gatekeeper

**File:** `internal/strategy/engine.go`
**Location:** Inside the main analysis loop, *before* running expensive calculations (like RS or Support detection).
**Logic:**
Exclude assets that have very low daily volatility (dead coins or stable-like behavior).

```go
// Calculate Normalized Daily Range (NDR)
// Formula: (High24h - Low24h) / CurrentPrice
dailyRange := ticker.HighPrice24h - ticker.LowPrice24h
volatility := dailyRange / currentPrice

// Threshold: 3% (0.03)
if volatility < 0.03 {
    return nil // REJECT: Asset is too stable/dead, not worth trading fees.
}

```

### 2.2 Scoring: "Pump Rollover" Bonus

**File:** `internal/strategy/engine.go` (inside `CalculateScore` function)
**Logic:**
Give a score bonus to assets that are **Green** on the daily timeframe (> +5%) but are currently generating a Breakdown signal. This targets the "long squeeze" scenario where day-traders are trapped.

```go
// BONUS: Pump Rollover (The "Hangover" Effect)
// If the coin is up > 5% in the last 24h, but we have a breakdown signal locally,
// it indicates a potential reversal of a pump. This is a high-quality setup.
if ticker.Price24hPcnt > 0.05 { // > +5%
    score += 15
}

```

---

## 3. Implementation Summary (Checklist)

1. **Update Models:** Ensure `Ticker` struct has 24h stats.
2. **Update Engine:**
* Add **Volatility Filter** (`NDR < 3%` -> Skip).
* Add **Pump Bonus** to Scoring (`24h Change > 5%` -> +15 pts).


3. **No Architecture Changes:** Continue using the existing RingBuffer and sliding window mechanism.

---

**Note on "30 Candle Window":**
Confirm in comments that the analysis runs on a **sliding window**. Even though we look back 30-60 minutes, the analysis is triggered on *every* new 1-minute candle close, ensuring real-time signal detection without delays.