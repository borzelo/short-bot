package strategy

import (
	"math"

	"github.com/islamtagirov/millionaire-bot/internal/models"
)

// Support detection constants (v1.4.0)
const (
	ConsolidationLookback = 60    // Look for consolidation over 60 candles
	ConsolidationMinBars  = 20    // Minimum 20 candles in consolidation
	ConsolidationMaxRange = 0.03  // Maximum 3% range for consolidation
	TouchThreshold        = 0.003 // 0.3% tolerance for "touching" a level
)

// DetectSupport finds support level using consolidation detection with fractal fallback (v1.4.0)
func DetectSupport(candles []models.Candle) *models.SupportLevel {
	// Priority 1: Consolidation breakout (stronger signal)
	if support := DetectConsolidation(candles); support != nil {
		return support
	}

	// Priority 2: Fractal Low (fallback - current logic)
	return DetectFractalLow(candles)
}

// DetectConsolidation finds a consolidation zone with multiple touches
func DetectConsolidation(candles []models.Candle) *models.SupportLevel {
	if len(candles) < ConsolidationLookback {
		return nil
	}

	recent := candles[len(candles)-ConsolidationLookback:]

	// Find High and Low of the period
	high, low := findHighLow(recent)
	if high == 0 || low == 0 {
		return nil
	}

	avgPrice := (high + low) / 2
	if avgPrice == 0 {
		return nil
	}

	rangePercent := (high - low) / avgPrice

	// If range > 3% - not a consolidation
	if rangePercent > ConsolidationMaxRange {
		return nil
	}

	// Count touches of the lower boundary
	touchCount := 0
	var totalVolumeAtTouches float64
	for _, c := range recent {
		if low > 0 && math.Abs(c.Low-low)/low < TouchThreshold {
			touchCount++
			totalVolumeAtTouches += c.Volume
		}
	}

	// Need at least 2 touches for a valid consolidation
	if touchCount < 2 {
		return nil
	}

	avgVolumeAtTouches := 0.0
	if touchCount > 0 {
		avgVolumeAtTouches = totalVolumeAtTouches / float64(touchCount)
	}

	return &models.SupportLevel{
		Price:           low,
		Timestamp:       recent[0].Timestamp,
		TouchCount:      touchCount,
		VolumeAtLevel:   avgVolumeAtTouches,
		RangeHigh:       high,
		RangeLow:        low,
		IsConsolidation: true,
	}
}

// DetectFractalLow finds support using fractal pattern (existing logic, enhanced)
func DetectFractalLow(candles []models.Candle) *models.SupportLevel {
	if len(candles) < SupportLookback {
		return nil
	}

	recentCandles := candles[len(candles)-SupportLookback:]

	// Find fractal lows: Low[i] < Low[i-2...i+2]
	var fractals []models.SupportLevel

	for i := 2; i < len(recentCandles)-2; i++ {
		low := recentCandles[i].Low
		isFractal := true

		for j := i - 2; j <= i+2; j++ {
			if j == i {
				continue
			}
			if recentCandles[j].Low < low {
				isFractal = false
				break
			}
		}

		if isFractal {
			// Count how many times this level was touched
			touchCount := countTouches(recentCandles, low)

			fractals = append(fractals, models.SupportLevel{
				Price:           low,
				Timestamp:       recentCandles[i].Timestamp,
				TouchCount:      touchCount,
				IsConsolidation: false,
			})
		}
	}

	if len(fractals) == 0 {
		return nil
	}

	// Return the most recent fractal low
	return &fractals[len(fractals)-1]
}

// findHighLow returns the highest high and lowest low in a candle slice
// Skips candles with zero or invalid prices
func findHighLow(candles []models.Candle) (high, low float64) {
	if len(candles) == 0 {
		return 0, 0
	}

	// Find first valid candle to initialize
	high = 0
	low = 0
	for _, c := range candles {
		if c.High > 0 && c.Low > 0 {
			high = c.High
			low = c.Low
			break
		}
	}

	// If no valid candles found
	if high == 0 || low == 0 {
		return 0, 0
	}

	// Find actual high and low
	for _, c := range candles {
		// Skip invalid candles
		if c.High <= 0 || c.Low <= 0 {
			continue
		}
		if c.High > high {
			high = c.High
		}
		if c.Low < low {
			low = c.Low
		}
	}

	return high, low
}

// countTouches counts how many candles touched a price level within threshold
func countTouches(candles []models.Candle, level float64) int {
	if level == 0 {
		return 0
	}

	count := 0
	for _, c := range candles {
		// Check if low is within threshold of the level
		if math.Abs(c.Low-level)/level < TouchThreshold {
			count++
		}
	}
	return count
}
