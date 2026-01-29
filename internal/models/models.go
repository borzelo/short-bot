package models

import "time"

// Candle represents a 1-minute candlestick
type Candle struct {
	Symbol    string
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// Asset represents a tradable instrument
type Asset struct {
	Symbol      string
	TickSize    float64
	MinLotSize  float64
	IsTradable  bool
	Volume24h   float64 // For sorting top 100
}

// Signal represents a breakdown signal
type Signal struct {
	ID               int
	CreatedAt        time.Time
	Symbol           string
	PriceTrigger     float64
	LevelBroken      float64
	BreakdownVolume  float64
	ScoreRS          float64
	ScoreTotal       int
	Meta             map[string]interface{}
}

// SupportLevel represents a detected support level (v1.4.0 enhanced)
type SupportLevel struct {
	Price           float64   // Support price level
	Timestamp       time.Time // When the level was formed (FormationTime)
	TouchCount      int       // How many times price touched this level
	VolumeAtLevel   float64   // Average volume at touches
	RangeHigh       float64   // Upper bound of consolidation zone
	RangeLow        float64   // Lower bound (= Price)
	IsConsolidation bool      // Whether this is a consolidation breakout
}

// MarketData holds aggregated market information for analysis
type MarketData struct {
	Symbol        string
	Candles       []Candle
	CurrentPrice  float64
	CurrentVolume float64
	AvgVolume     float64 // Average volume over last N candles
	Support       *SupportLevel
}

// Ticker24hStats holds 24h price statistics for volatility filter and pump detection
type Ticker24hStats struct {
	Symbol       string
	HighPrice24h float64 // Highest price in last 24h
	LowPrice24h  float64 // Lowest price in last 24h
	Price24hPcnt float64 // 24h price change percentage (e.g., 0.05 = +5%)
	LastPrice    float64 // Current/last price
	OpenInterest float64 // Open Interest in contracts (v1.5.0)
}

// OISnapshot stores Open Interest snapshot for delta calculation (v1.5.0)
type OISnapshot struct {
	OpenInterest float64
	Timestamp    time.Time
}

// TrainingData represents a data point for ML model training (v1.9.0)
// Captures ALL breakdown events (structure) regardless of quality filters
type TrainingData struct {
	ID        int
	CreatedAt time.Time
	Symbol    string

	// ENTRY POINT (T=0)
	EntryPrice         float64 // Price at detection moment
	EntrySupportLevel  float64 // Support level that was broken

	// FEATURES (INPUTS) - What the model learns from
	// All indicators and filters calculated at T=0
	Features map[string]interface{} // JSONB: RS, Volume, Funding, ATR, TimeOfDay, etc.

	// RESULTS AFTER 15 MINUTES (OUTPUTS)
	// Filled by background worker after 15 min
	Price15mMax   *float64 // High in 15 min (for Stop-Loss check)
	Price15mMin   *float64 // Low in 15 min (for Take-Profit check)
	Price15mClose *float64 // Close price after 15 min

	// RESULTS AFTER 60 MINUTES (OUTPUTS)
	// Filled by background worker after 60 min
	Price60mMax   *float64 // High in 60 min
	Price60mMin   *float64 // Low in 60 min
	Price60mClose *float64 // Close price after 60 min

	// META INFORMATION
	IsShadowMode  bool     // TRUE if this is a "soft" signal for learning (failed quality filters)
	MLPrediction  *float64 // (Future) Model prediction if available
}

// TrainingOutcomes holds the 15m/60m price outcomes for a training data point (v1.9.0)
type TrainingOutcomes struct {
	Price15mMax   *float64
	Price15mMin   *float64
	Price15mClose *float64
	Price60mMax   *float64
	Price60mMin   *float64
	Price60mClose *float64
}
