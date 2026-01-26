package strategy

import (
	"github.com/islamtagirov/millionaire-bot/internal/models"
)

// Bounceback check constants (v1.4.0)
const (
	MaxWickBouncePercent = 0.5 // Wick should not recover more than 50% of the candle body
	MinBreakdownDepth    = 0.001 // Minimum 0.1% below support to count as breakdown
)

// ConfirmNoBounceback verifies that the current breakdown candle doesn't have a strong buyback wick
// Returns true if breakdown is confirmed (no significant bounce), false otherwise
// Checks the CURRENT candle's wick, not future candles
func ConfirmNoBounceback(candles []models.Candle, supportPrice float64) bool {
	if len(candles) == 0 {
		return false
	}

	currentCandle := candles[len(candles)-1]

	// Verify this is actually a breakdown (close below support)
	if currentCandle.Close >= supportPrice {
		return false
	}

	// Calculate breakdown depth
	breakdownDepth := supportPrice - currentCandle.Close
	if supportPrice > 0 && breakdownDepth/supportPrice < MinBreakdownDepth {
		return false // Breakdown too shallow
	}

	// Check if the candle has a strong buyback wick
	// A strong buyback wick means the candle dipped lower but recovered significantly
	candleBody := currentCandle.Open - currentCandle.Close
	if candleBody <= 0 {
		// Green candle during breakdown - suspicious, but allow if close is below support
		return true
	}

	// Calculate how much the wick bounced from the low
	wickBounce := currentCandle.Close - currentCandle.Low
	if wickBounce > candleBody*MaxWickBouncePercent {
		// Strong buyback wick - price dipped but recovered too much
		return false
	}

	return true
}

// AnalyzeBreakdownQuality returns additional quality metrics for the breakdown
func AnalyzeBreakdownQuality(candles []models.Candle, support *models.SupportLevel) BreakdownQuality {
	quality := BreakdownQuality{}

	if len(candles) == 0 || support == nil {
		return quality
	}

	currentCandle := candles[len(candles)-1]

	// How far below support the price closed (%)
	if support.Price > 0 {
		quality.BreakdownDepth = (support.Price - currentCandle.Close) / support.Price * 100
	}

	// Volume spike compared to average
	if len(candles) >= VolumeAvgWindow {
		avgVol := calculateAvgVolumeFromCandles(candles)
		if avgVol > 0 {
			quality.VolumeSpike = currentCandle.Volume / avgVol
		}
	}

	// Momentum: compare close to open
	if currentCandle.Open > 0 {
		quality.Momentum = (currentCandle.Close - currentCandle.Open) / currentCandle.Open * 100
	}

	// Support age in minutes
	quality.SupportAgeMinutes = support.Timestamp.Sub(currentCandle.Timestamp).Minutes() * -1

	// Is consolidation break (stronger signal)
	quality.IsConsolidationBreak = support.IsConsolidation

	// Number of times support was tested
	quality.SupportTouchCount = support.TouchCount

	return quality
}

// BreakdownQuality holds quality metrics for a breakdown signal
type BreakdownQuality struct {
	BreakdownDepth       float64 // How far below support (%)
	VolumeSpike          float64 // Current volume vs average
	Momentum             float64 // Close vs Open change (%)
	SupportAgeMinutes    float64 // How old is the support level
	IsConsolidationBreak bool    // Is this a consolidation breakdown
	SupportTouchCount    int     // How many times support was touched
}

// calculateAvgVolumeFromCandles calculates average volume from candle slice
func calculateAvgVolumeFromCandles(candles []models.Candle) float64 {
	window := VolumeAvgWindow
	if len(candles) < window {
		window = len(candles)
	}
	if window == 0 {
		return 0
	}

	var sum float64
	for i := len(candles) - window; i < len(candles); i++ {
		sum += candles[i].Volume
	}
	return sum / float64(window)
}
