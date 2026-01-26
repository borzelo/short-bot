package models

import "time"

// WeaknessScore represents the fundamental weakness metrics of an asset (v1.4.0)
// Used for selecting fundamentally weak assets instead of just mid-cap by volume
type WeaknessScore struct {
	Symbol        string    // Trading symbol (e.g., "SOLUSDT")
	RS7d          float64   // Relative Strength vs BTC over 7 days (%)
	RS24h         float64   // Relative Strength vs BTC over 24 hours (%)
	PriceVsMA50   float64   // (Price - MA50) / MA50 * 100 (%)
	PriceVsMA200  float64   // (Price - MA200) / MA200 * 100 (%)
	VolumeDecline float64   // (AvgVol7d - AvgVol30d) / AvgVol30d * 100 (%)
	FundingRate   float64   // Current funding rate (%)
	TotalScore    float64   // Total weakness score (0-100)
	UpdatedAt     time.Time // When the score was last calculated
}

// HistoricalCandle represents a daily/hourly candle for historical analysis
type HistoricalCandle struct {
	Symbol    string
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Turnover  float64
}
