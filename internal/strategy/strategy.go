package strategy

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

const (
	MaxCandlesInMemory = 240 // 4 hours of 1-minute candles (keep for history)
	RSLookbackMinutes  = 60  // 1 hour for RS calculation (was 4h, too conservative)
	MinRSLookback      = 30  // Minimum 30 minutes to start analysis
	SupportLookback    = 30  // 30 minutes for support detection (was 60m)
	VolumeAvgWindow    = 20  // 20 candles for volume average
)

type Engine struct {
	mu          sync.RWMutex
	candleCache map[string][]models.Candle // symbol -> ring buffer of candles
	btcCandles  []models.Candle            // BTC candles for RS calculation
	signalChan  chan *models.Signal
}

func NewEngine() *Engine {
	return &Engine{
		candleCache: make(map[string][]models.Candle),
		btcCandles:  make([]models.Candle, 0),
		signalChan:  make(chan *models.Signal, 100),
	}
}

func (e *Engine) ProcessCandle(candle models.Candle) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Store candle in cache
	if candle.Symbol == "BTCUSDT" {
		e.btcCandles = append(e.btcCandles, candle)
		if len(e.btcCandles) > MaxCandlesInMemory {
			e.btcCandles = e.btcCandles[1:]
		}
	}

	if _, exists := e.candleCache[candle.Symbol]; !exists {
		e.candleCache[candle.Symbol] = make([]models.Candle, 0, MaxCandlesInMemory)
	}

	e.candleCache[candle.Symbol] = append(e.candleCache[candle.Symbol], candle)
	if len(e.candleCache[candle.Symbol]) > MaxCandlesInMemory {
		e.candleCache[candle.Symbol] = e.candleCache[candle.Symbol][1:]
	}

	// Skip analysis if not enough data
	// Use minimum 30 minutes to start, but prefer 60 minutes for better accuracy
	minDataPoints := MinRSLookback
	assetDataPoints := len(e.candleCache[candle.Symbol])
	btcDataPoints := len(e.btcCandles)

	if assetDataPoints < minDataPoints || btcDataPoints < minDataPoints {
		// Log progress every 10 candles
		if assetDataPoints%10 == 0 {
			log.Debug().
				Str("symbol", candle.Symbol).
				Int("asset_candles", assetDataPoints).
				Int("btc_candles", btcDataPoints).
				Int("needed", minDataPoints).
				Msg("collecting data before analysis starts")
		}
		return
	}

	// Log when we start analyzing a new symbol for the first time
	if assetDataPoints == minDataPoints {
		log.Info().
			Str("symbol", candle.Symbol).
			Int("candles", assetDataPoints).
			Msg("started analysis for symbol - enough data collected")
	}

	// Analyze for breakdown signal
	signal := e.analyzeBreakdown(candle.Symbol)
	if signal != nil {
		select {
		case e.signalChan <- signal:
			log.Info().
				Str("symbol", signal.Symbol).
				Int("score", signal.ScoreTotal).
				Msg("signal generated")
		default:
			log.Warn().Str("symbol", candle.Symbol).Msg("signal channel full")
		}
	}
}

func (e *Engine) analyzeBreakdown(symbol string) *models.Signal {
	candles := e.candleCache[symbol]
	if len(candles) == 0 {
		return nil
	}

	currentCandle := candles[len(candles)-1]

	// 1. Calculate Relative Strength (RS)
	rs := e.calculateRS(symbol)

	// 2. Detect support level
	support := e.detectSupport(candles)
	if support == nil {
		return nil
	}

	// 3. Check breakdown condition: Close < Support
	if currentCandle.Close >= support.Price {
		return nil
	}

	// 4. Calculate average volume
	avgVolume := e.calculateAvgVolume(candles)
	if avgVolume == 0 {
		return nil
	}

	volumeRatio := currentCandle.Volume / avgVolume

	// 5. Check volume condition: Volume > 1.5x average
	if volumeRatio < 1.5 {
		return nil
	}

	// 6. Calculate score
	score := e.calculateScore(rs, volumeRatio, support, currentCandle)

	// Create signal
	signal := &models.Signal{
		CreatedAt:       time.Now(),
		Symbol:          symbol,
		PriceTrigger:    currentCandle.Close,
		LevelBroken:     support.Price,
		BreakdownVolume: currentCandle.Volume,
		ScoreRS:         rs,
		ScoreTotal:      score,
		Meta: map[string]interface{}{
			"volume_ratio":         volumeRatio,
			"support_age_minutes":  time.Since(support.Timestamp).Minutes(),
			"support_touch_count":  support.TouchCount,
			"avg_volume":           avgVolume,
		},
	}

	return signal
}

func (e *Engine) calculateRS(symbol string) float64 {
	if symbol == "BTCUSDT" {
		return 0 // BTC has no RS against itself
	}

	assetCandles := e.candleCache[symbol]
	if len(assetCandles) < MinRSLookback || len(e.btcCandles) < MinRSLookback {
		return 0
	}

	// Adaptive lookback: use what we have, but prefer RSLookbackMinutes (60m)
	// If we only have 30-60 minutes of data, use it instead of waiting for full 60m
	lookback := RSLookbackMinutes
	if len(assetCandles) < lookback {
		lookback = len(assetCandles)
	}
	if len(e.btcCandles) < lookback {
		lookback = len(e.btcCandles)
	}

	// Get price change over the lookback period
	assetOld := assetCandles[len(assetCandles)-lookback].Close
	assetNew := assetCandles[len(assetCandles)-1].Close
	assetChange := ((assetNew - assetOld) / assetOld) * 100

	btcOld := e.btcCandles[len(e.btcCandles)-lookback].Close
	btcNew := e.btcCandles[len(e.btcCandles)-1].Close
	btcChange := ((btcNew - btcOld) / btcOld) * 100

	// RS = Asset % Change - BTC % Change
	return assetChange - btcChange
}

func (e *Engine) detectSupport(candles []models.Candle) *models.SupportLevel {
	if len(candles) < SupportLookback {
		return nil
	}

	recentCandles := candles[len(candles)-SupportLookback:]

	// Find fractal lows: Low[i] < Low[i-2...i+2]
	var fractals []models.SupportLevel

	for i := 2; i < len(recentCandles)-2; i++ {
		low := recentCandles[i].Low
		isFractal := true

		// Check if this low is lower than surrounding candles
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
			fractals = append(fractals, models.SupportLevel{
				Price:     low,
				Timestamp: recentCandles[i].Timestamp,
			})
		}
	}

	if len(fractals) == 0 {
		return nil
	}

	// Return the most recent fractal low
	mostRecent := fractals[len(fractals)-1]
	return &mostRecent
}

func (e *Engine) calculateAvgVolume(candles []models.Candle) float64 {
	window := VolumeAvgWindow
	if len(candles) < window {
		window = len(candles)
	}

	recentCandles := candles[len(candles)-window:]
	var sum float64
	for _, c := range recentCandles {
		sum += c.Volume
	}

	return sum / float64(len(recentCandles))
}

func (e *Engine) calculateScore(rs float64, volumeRatio float64, support *models.SupportLevel, currentCandle models.Candle) int {
	score := 0

	// Base: 0

	// Weakness scoring
	if rs < -3.0 {
		score += 30
		if rs < -5.0 {
			score += 10 // Extra 10 for very weak
		}
	}

	// Volume scoring
	if volumeRatio > 2.0 {
		score += 20
		if volumeRatio > 3.0 {
			score += 10 // Extra 10 for exceptional volume
		}
	}

	// Level Age scoring
	supportAgeMinutes := time.Since(support.Timestamp).Minutes()
	if supportAgeMinutes > 30 {
		score += 20
	}

	// Trend divergence scoring: BTC Green but Coin Red
	// Get BTC current change (1 candle)
	if len(e.btcCandles) >= 2 {
		btcPrevClose := e.btcCandles[len(e.btcCandles)-2].Close
		btcCurrentClose := e.btcCandles[len(e.btcCandles)-1].Close
		btcChange := ((btcCurrentClose - btcPrevClose) / btcPrevClose) * 100

		// Get asset current change
		assetCandles := e.candleCache[currentCandle.Symbol]
		if len(assetCandles) >= 2 {
			assetPrevClose := assetCandles[len(assetCandles)-2].Close
			assetCurrentClose := currentCandle.Close
			assetChange := ((assetCurrentClose - assetPrevClose) / assetPrevClose) * 100

			// BTC is green (>0%) but asset is red (<0%)
			if btcChange > 0 && assetChange < 0 {
				score += 10
			}
		}
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	return score
}

func (e *Engine) GetSignalChannel() <-chan *models.Signal {
	return e.signalChan
}

func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := map[string]interface{}{
		"tracked_symbols": len(e.candleCache),
		"btc_candles":     len(e.btcCandles),
	}

	return stats
}

// Helper: Format float with precision
func formatFloat(f float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(f*ratio) / ratio
}

// GetMarketData returns current market data for a symbol (for debugging)
func (e *Engine) GetMarketData(symbol string) (*models.MarketData, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	candles, exists := e.candleCache[symbol]
	if !exists || len(candles) == 0 {
		return nil, fmt.Errorf("no data for symbol %s", symbol)
	}

	currentCandle := candles[len(candles)-1]
	avgVolume := e.calculateAvgVolume(candles)
	support := e.detectSupport(candles)

	return &models.MarketData{
		Symbol:        symbol,
		Candles:       candles,
		CurrentPrice:  currentCandle.Close,
		CurrentVolume: currentCandle.Volume,
		AvgVolume:     avgVolume,
		Support:       support,
	}, nil
}
