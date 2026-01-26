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

// SupportLevel represents a detected support level
type SupportLevel struct {
	Price     float64
	Timestamp time.Time
	TouchCount int
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
