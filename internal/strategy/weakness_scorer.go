package strategy

import (
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
)

// WeaknessScorer calculates weakness scores for assets (v1.4.0)
// Higher score = weaker asset = better short candidate
type WeaknessScorer struct{}

// NewWeaknessScorer creates a new WeaknessScorer
func NewWeaknessScorer() *WeaknessScorer {
	return &WeaknessScorer{}
}

// CalculateScore computes the total weakness score based on multiple factors
func (ws *WeaknessScorer) CalculateScore(
	rs7d float64,
	rs24h float64,
	priceVsMA50 float64,
	priceVsMA200 float64,
	volumeDecline float64,
	fundingRate float64,
) *models.WeaknessScore {
	score := 0.0

	// RS7d: the lower, the weaker the asset (up to 30 points)
	if rs7d < -20 {
		score += 30
	} else if rs7d < -10 {
		score += 20
	} else if rs7d < -5 {
		score += 10
	}

	// RS24h: short-term weakness (up to 15 points)
	if rs24h < -10 {
		score += 15
	} else if rs24h < -5 {
		score += 10
	} else if rs24h < -2 {
		score += 5
	}

	// Price below MA50 (up to 20 points)
	if priceVsMA50 < -20 {
		score += 20
	} else if priceVsMA50 < -10 {
		score += 15
	} else if priceVsMA50 < -5 {
		score += 10
	}

	// Price below MA200 (up to 15 points)
	if priceVsMA200 < -30 {
		score += 15
	} else if priceVsMA200 < -15 {
		score += 10
	}

	// Declining volume - asset losing interest (up to 10 points)
	if volumeDecline < -30 {
		score += 10
	} else if volumeDecline < -15 {
		score += 5
	}

	// Positive funding - longs paying, potential for drop (up to 10 points)
	if fundingRate > 0.05 {
		score += 10
	} else if fundingRate > 0.01 {
		score += 5
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	return &models.WeaknessScore{
		RS7d:          rs7d,
		RS24h:         rs24h,
		PriceVsMA50:   priceVsMA50,
		PriceVsMA200:  priceVsMA200,
		VolumeDecline: volumeDecline,
		FundingRate:   fundingRate,
		TotalScore:    score,
		UpdatedAt:     time.Now(),
	}
}

// CalculateMA calculates Moving Average from historical candles
func CalculateMA(candles []models.HistoricalCandle, period int) float64 {
	if len(candles) < period {
		return 0
	}

	sum := 0.0
	for i := len(candles) - period; i < len(candles); i++ {
		sum += candles[i].Close
	}
	return sum / float64(period)
}

// CalculateRS7d calculates 7-day Relative Strength vs BTC
func CalculateRS7d(assetCandles, btcCandles []models.HistoricalCandle) float64 {
	if len(assetCandles) < 7 || len(btcCandles) < 7 {
		return 0
	}

	assetOldIdx := len(assetCandles) - 7
	assetNewIdx := len(assetCandles) - 1
	btcOldIdx := len(btcCandles) - 7
	btcNewIdx := len(btcCandles) - 1

	if assetCandles[assetOldIdx].Close == 0 || btcCandles[btcOldIdx].Close == 0 {
		return 0
	}

	assetChange := (assetCandles[assetNewIdx].Close - assetCandles[assetOldIdx].Close) /
		assetCandles[assetOldIdx].Close * 100

	btcChange := (btcCandles[btcNewIdx].Close - btcCandles[btcOldIdx].Close) /
		btcCandles[btcOldIdx].Close * 100

	return assetChange - btcChange
}

// CalculateRS24h calculates 24-hour Relative Strength vs BTC
// Uses 4h candles (6 candles = 24h)
func CalculateRS24h(assetCandles, btcCandles []models.HistoricalCandle) float64 {
	// For 4h candles, we need 6 candles for 24h
	const candlesNeeded = 6
	if len(assetCandles) < candlesNeeded || len(btcCandles) < candlesNeeded {
		return 0
	}

	assetOldIdx := len(assetCandles) - candlesNeeded
	assetNewIdx := len(assetCandles) - 1
	btcOldIdx := len(btcCandles) - candlesNeeded
	btcNewIdx := len(btcCandles) - 1

	if assetCandles[assetOldIdx].Close == 0 || btcCandles[btcOldIdx].Close == 0 {
		return 0
	}

	assetChange := (assetCandles[assetNewIdx].Close - assetCandles[assetOldIdx].Close) /
		assetCandles[assetOldIdx].Close * 100

	btcChange := (btcCandles[btcNewIdx].Close - btcCandles[btcOldIdx].Close) /
		btcCandles[btcOldIdx].Close * 100

	return assetChange - btcChange
}

// CalculatePriceVsMA calculates (Price - MA) / MA * 100
func CalculatePriceVsMA(currentPrice, ma float64) float64 {
	if ma == 0 {
		return 0
	}
	return (currentPrice - ma) / ma * 100
}

// CalculateVolumeDecline calculates (AvgVol7d - AvgVol30d) / AvgVol30d * 100
func CalculateVolumeDecline(candles []models.HistoricalCandle) float64 {
	if len(candles) < 30 {
		return 0
	}

	// Calculate 7-day average volume
	var sum7d float64
	for i := len(candles) - 7; i < len(candles); i++ {
		sum7d += candles[i].Volume
	}
	avgVol7d := sum7d / 7

	// Calculate 30-day average volume
	var sum30d float64
	for i := len(candles) - 30; i < len(candles); i++ {
		sum30d += candles[i].Volume
	}
	avgVol30d := sum30d / 30

	if avgVol30d == 0 {
		return 0
	}

	return (avgVol7d - avgVol30d) / avgVol30d * 100
}
