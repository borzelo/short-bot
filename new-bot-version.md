# Role
You are a Senior Go Developer specializing in High-Frequency Trading (HFT) systems.

# Context
We are upgrading a trading bot (v1.7.1) implementing the "Short Breakdown on Weak Assets" strategy.
The current architecture uses a `RingBuffer` for candles, a `Strategy Engine` for signal detection, and PostgreSQL for storage.
Recently, we identified a logic flaw: the bot signals shorts on assets that are already oversold (e.g., dropped >15% in 24h) or extended too far from the mean, leading to "bear trap" losses.

# Task
Implement 3 specific filters to prevent late entries on exhausted trends. Modify `internal/strategy` and `internal/models` as needed.

# Specific Requirements

## 1. Oversold Filter (24h Fatigue)
**Goal:** Prevent shorting assets that have already crashed significantly.
**Implementation:**
- In `internal/strategy/engine.go` (or where filters are applied):
- Check `Ticker24hStats.Price24hPcnt`.
- **Logic:** If `Price24hPcnt < -0.15` (lower than -15%), **discard the signal immediately** (return nil).
- Add a log message: "Signal rejected: asset oversold (-XX%)".

## 2. Dynamic Support Age (Volatility Adjustment)
**Goal:** Require older, stronger support levels during high volatility to avoid shorting temporary pauses.
**Implementation:**
- In the `Support Detection` logic:
- Calculate `Change1h` (Price change over the last 60 minutes based on candles in RingBuffer).
- Define dynamic `minAge`:
    - IF `Change1h` drop is > 2% (value < -0.02): Set `minAge = 45 minutes`.
    - ELSE: Set `minAge = 15 minutes` (default).
- Update the condition `if supportAge < minAge { return }`.

## 3. MA Extension Filter (Rubber Band Effect)
**Goal:** Prevent shorting when price is too far below the average (risk of mean reversion).
**Implementation:**
- **Math Helper:** Ensure a function exists to calculate SMA (Simple Moving Average) from a slice of floats.
- **In Engine:**
    - Calculate `SMA_200` using the last 200 closing prices from the `RingBuffer`.
    - If `RingBuffer` has fewer than 200 candles, skip this filter (or use available max).
    - Calculate Deviation: `diff = (SMA_200 - CurrentClose) / SMA_200`.
    - **Logic:** If `diff > 0.04` (Price is >4% below SMA_200), **discard the signal**.
    - Add a log message: "Signal rejected: price extended from SMA200 by X%".

## Code Structure Constraints
- Keep the code allocation-free where possible (reuse existing buffers).
- Add these checks **early** in the pipeline (fail fast principle).
- Ensure all "Magic Numbers" (0.15, 0.04, 45, 200) are defined as constants at the top of the package or in `config`.

# Output
Provide the modified Go code blocks for:
1. Constants definition.
2. The logic implementation within the `analyzeBreakdown` (or equivalent) function.
3. Helper functions for SMA and Volatility calculation if they don't exist.