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
