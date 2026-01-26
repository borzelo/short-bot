
# SPECIFICATION: Strategy Logic Upgrade (v1.1)

**Context:** Upgrade the existing Go trading bot ("Millionaire Bot") to align with the "OBVAL" strategy.
**Goal:** Shift focus from Top-Tier assets to volatile Mid-Caps and add protection against "short squeezes" and "buybacks".

## 1. Module: Market Data (`internal/bybit`)

### 1.1 Filter Asset Universe

**File:** `internal/bybit/api.go`
**Action:** Modify the symbol fetching logic to exclude "Heavyweights".
**Logic:**

1. Fetch all USDT Perpetual symbols.
2. Sort by 24h Turnover (Volume).
3. **Exclude** the Top 15 assets by volume (e.g., BTC, ETH, SOL, BNB, XRP, DOGE...).
4. **Select** the next 100 assets (Rank 16 to 116).
5. *Constraint:* Volume must still be > $10M/24h (to avoid dead coins).

### 1.2 Fetch Funding Rates

**File:** `internal/bybit/api.go`
**Action:** Implement a method `GetFundingRates(symbols []string) map[string]float64`.
**Details:**

* Use Bybit V5 endpoint: `GET /v5/market/tickers`.
* Field: `fundingRate`.
* Update this data periodically (e.g., every 5 minutes) via a background ticker in `main.go`.

---

## 2. Module: Strategy Engine (`internal/strategy`)

### 2.1 Filter: Funding Rate (Anti-Squeeze)

**File:** `internal/strategy/engine.go`
**Action:** Add a "Crowded Short" check before generating a signal.
**Logic:**

```go
// If funding is highly negative, shorts are paying longs.
// This indicates a crowded trade and high squeeze risk.
if fundingRate < -0.015 { // Threshold: -0.015%
    return nil // REJECT SIGNAL
}

```

### 2.2 Confirmation: Wick Analysis (No Buyback)

**File:** `internal/strategy/engine.go`
**Action:** Refine the Breakdown Trigger logic. We must ensure the candle closed near the bottom, not just below support.
**Formula:**

```go
candleRange := candle.High - candle.Low
closePosition := (candle.Close - candle.Low) / candleRange

// If closePosition > 0.3, it means the price bounced back up from the low (long lower wick).
// We want the price to close in the bottom 30% of the candle.
if closePosition > 0.3 {
    return nil // REJECT SIGNAL (Too much buy pressure)
}

```

### 2.3 Scoring Update

**Action:** Update `CalculateScore` logic.

* **Add (+20 pts):** If `FundingRate > 0.01%` (Positive funding = Longs paying shorts = Healthy for breakdown).
* **Add (+10 pts):** If `ClosePosition < 0.1` (Closed at the very dead bottom).

---

## 3. Summary of Changes

1. **Assets:** Top 100 → Mid-Cap Volatile (Exclude Top 15).
2. **Safety:** Reject if `Funding < -0.015%`.
3. **Pattern:** Reject if Candle Wick > 30% of body (Buyback detected).