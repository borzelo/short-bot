package utils

import (
	"math"
)

// StdDev calculates the standard deviation of a slice of float64 values
// Returns 0 if the slice has fewer than 2 elements
func StdDev(values []float64) float64 {
	n := len(values)
	if n < 2 {
		return 0
	}

	mean := Mean(values)
	var sumSquares float64
	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}

	// Use sample standard deviation (n-1)
	return math.Sqrt(sumSquares / float64(n-1))
}

// Mean calculates the arithmetic mean of a slice of float64 values
// Returns 0 if the slice is empty
func Mean(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}

	var sum float64
	for _, v := range values {
		sum += v
	}

	return sum / float64(n)
}

// ZScore calculates the Z-Score of a value given mean and standard deviation
// Returns 0 if stdDev is 0 (to avoid division by zero)
func ZScore(value, mean, stdDev float64) float64 {
	if stdDev == 0 {
		return 0
	}
	return (value - mean) / stdDev
}

// CalculateVolumeZScore calculates the Z-Score for current volume against historical volumes
// Returns Z-Score and mean volume (for additional context)
func CalculateVolumeZScore(currentVolume float64, historicalVolumes []float64) (zScore float64, mean float64, stdDev float64) {
	if len(historicalVolumes) < 2 {
		return 0, 0, 0
	}

	mean = Mean(historicalVolumes)
	stdDev = StdDev(historicalVolumes)

	if stdDev == 0 {
		return 0, mean, 0
	}

	zScore = ZScore(currentVolume, mean, stdDev)
	return zScore, mean, stdDev
}

// DistanceFromHigh calculates how far the current price is from the 24h high
// Returns percentage as decimal (e.g., 0.05 = 5% below high)
// Returns 0 if currentPrice >= high24h (at or above high)
func DistanceFromHigh(currentPrice, high24h float64) float64 {
	if high24h <= 0 {
		return 0
	}
	// Edge case: if current price is at or above high, distance is 0
	if currentPrice >= high24h {
		return 0
	}
	return (high24h - currentPrice) / high24h
}

// SMA calculates Simple Moving Average for a slice of float64 values
// Returns 0 if the slice is empty
// v1.8.0: Added for MA Extension Filter
func SMA(values []float64) float64 {
	return Mean(values) // SMA is just the arithmetic mean
}

// SMAFromPrices calculates SMA from closing prices
// Takes a slice of closing prices and returns SMA
// v1.8.0: Helper for calculating SMA from candle data
func SMAFromPrices(closePrices []float64, window int) float64 {
	if len(closePrices) == 0 {
		return 0
	}
	// Use up to 'window' most recent prices
	start := 0
	if len(closePrices) > window {
		start = len(closePrices) - window
	}
	return SMA(closePrices[start:])
}

// CalculateDeviation calculates percentage deviation of current price from SMA
// Returns positive value if price is below SMA (e.g., 0.04 = 4% below)
// Returns negative value if price is above SMA
// v1.8.0: For MA Extension Filter (rubber band effect)
func CalculateDeviation(currentPrice, sma float64) float64 {
	if sma <= 0 {
		return 0
	}
	return (sma - currentPrice) / sma
}
