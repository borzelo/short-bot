package strategy

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

// Configuration constants
const (
	MaxCandlesInMemory = 240 // 4 hours of 1-minute candles
	RSLookbackMinutes  = 60  // 1 hour for RS calculation
	MinRSLookback      = 30  // Minimum 30 minutes to start analysis
	SupportLookback    = 60  // 60 minutes for support detection (was 30 - bug fix)
	VolumeAvgWindow    = 20  // 20 candles for volume average
	SignalCooldownMins = 15  // Cooldown between signals for same symbol
)

// Scoring thresholds
const (
	RSWeakThreshold     = -3.0  // Minimum RS to consider asset weak
	RSVeryWeakThreshold = -5.0  // RS threshold for extra weakness points
	VolumeMinRatio      = 1.5   // Minimum volume ratio for signal
	VolumeMediumRatio   = 2.0   // Medium volume ratio for scoring
	VolumeHighRatio     = 3.0   // High volume ratio for extra points
	ClosePositionMax    = 0.3   // Maximum close position (bottom 30%)
	ClosePositionStrong = 0.1   // Strong close position for bonus
	FundingAntiSqueeze  = -0.015 // Funding rate floor (anti-squeeze)
	FundingPositive     = 0.01  // Positive funding threshold for bonus
	SupportAgeBonus     = 20.0  // Minutes for support age bonus
	MinScoreForSignal   = 50    // Minimum score to generate signal

	// v1.3.0: Volatility filter and Pump Rollover
	MinVolatility24h      = 0.03  // Minimum 24h volatility (3%) to avoid dead coins
	PumpRolloverThreshold = 0.05  // 24h price change > 5% for pump bonus
	PumpRolloverBonus     = 15    // Bonus points for pump rollover scenario
)

// Engine processes candles and generates signals
type Engine struct {
	mu             sync.RWMutex
	candleCache    map[string]*RingBuffer          // symbol -> ring buffer of candles
	btcBuffer      *RingBuffer                     // BTC candles for RS calculation
	fundingRates   map[string]float64              // symbol -> funding rate (percent)
	ticker24hStats map[string]*models.Ticker24hStats // symbol -> 24h price statistics (v1.3.0)
	signalChan     chan *models.Signal
	lastSignal     map[string]time.Time            // symbol -> last signal time (cooldown)
}

// RingBuffer is a memory-efficient circular buffer for candles
type RingBuffer struct {
	data  []models.Candle
	size  int
	head  int
	count int
}

// NewRingBuffer creates a new ring buffer with given capacity
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data: make([]models.Candle, capacity),
		size: capacity,
	}
}

// Push adds a candle to the buffer, overwriting oldest if full
func (rb *RingBuffer) Push(c models.Candle) {
	rb.data[rb.head] = c
	rb.head = (rb.head + 1) % rb.size
	if rb.count < rb.size {
		rb.count++
	}
}

// Len returns the number of candles in the buffer
func (rb *RingBuffer) Len() int {
	return rb.count
}

// Get returns candle at logical index (0 = oldest, Len()-1 = newest)
func (rb *RingBuffer) Get(i int) models.Candle {
	if i < 0 || i >= rb.count {
		return models.Candle{}
	}
	start := (rb.head - rb.count + rb.size) % rb.size
	return rb.data[(start+i)%rb.size]
}

// Last returns the most recent candle
func (rb *RingBuffer) Last() models.Candle {
	if rb.count == 0 {
		return models.Candle{}
	}
	return rb.Get(rb.count - 1)
}

// GetRange returns candles from startIdx to endIdx (exclusive)
func (rb *RingBuffer) GetRange(startIdx, endIdx int) []models.Candle {
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx > rb.count {
		endIdx = rb.count
	}
	if startIdx >= endIdx {
		return nil
	}

	result := make([]models.Candle, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		result[i-startIdx] = rb.Get(i)
	}
	return result
}

// LastN returns the last n candles (newest last)
func (rb *RingBuffer) LastN(n int) []models.Candle {
	if n > rb.count {
		n = rb.count
	}
	return rb.GetRange(rb.count-n, rb.count)
}

func NewEngine() *Engine {
	return &Engine{
		candleCache:    make(map[string]*RingBuffer),
		btcBuffer:      NewRingBuffer(MaxCandlesInMemory),
		fundingRates:   make(map[string]float64),
		ticker24hStats: make(map[string]*models.Ticker24hStats),
		signalChan:     make(chan *models.Signal, 100),
		lastSignal:     make(map[string]time.Time),
	}
}

func (e *Engine) ProcessCandle(candle models.Candle) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Store BTC candles in dedicated buffer (not duplicated in candleCache)
	if candle.Symbol == "BTCUSDT" {
		e.btcBuffer.Push(candle)
		// BTC is only for RS calculation, not for signals
		return
	}

	// Store candle in symbol-specific buffer
	if _, exists := e.candleCache[candle.Symbol]; !exists {
		e.candleCache[candle.Symbol] = NewRingBuffer(MaxCandlesInMemory)
	}
	e.candleCache[candle.Symbol].Push(candle)

	// Check if we have enough data for analysis
	assetCount := e.candleCache[candle.Symbol].Len()
	btcCount := e.btcBuffer.Len()

	if assetCount < MinRSLookback || btcCount < MinRSLookback {
		if assetCount%10 == 0 {
			log.Debug().
				Str("symbol", candle.Symbol).
				Int("asset_candles", assetCount).
				Int("btc_candles", btcCount).
				Int("needed", MinRSLookback).
				Msg("collecting data before analysis starts")
		}
		return
	}

	// Log when analysis starts for a symbol
	if assetCount == MinRSLookback {
		log.Info().
			Str("symbol", candle.Symbol).
			Int("candles", assetCount).
			Msg("started analysis for symbol - enough data collected")
	}

	// Analyze for breakdown signal
	signal := e.analyzeBreakdown(candle.Symbol)
	if signal != nil {
		e.emitSignal(signal)
	}
}

func (e *Engine) emitSignal(signal *models.Signal) {
	// Check cooldown
	if lastTime, exists := e.lastSignal[signal.Symbol]; exists {
		if time.Since(lastTime).Minutes() < SignalCooldownMins {
			log.Debug().
				Str("symbol", signal.Symbol).
				Float64("minutes_since_last", time.Since(lastTime).Minutes()).
				Msg("signal skipped due to cooldown")
			return
		}
	}

	select {
	case e.signalChan <- signal:
		e.lastSignal[signal.Symbol] = time.Now()
		log.Info().
			Str("symbol", signal.Symbol).
			Int("score", signal.ScoreTotal).
			Float64("rs", signal.ScoreRS).
			Float64("price", signal.PriceTrigger).
			Float64("support", signal.LevelBroken).
			Msg("✅ HIGH-QUALITY SIGNAL GENERATED")
	default:
		log.Warn().Str("symbol", signal.Symbol).Msg("signal channel full")
	}
}

func (e *Engine) analyzeBreakdown(symbol string) *models.Signal {
	buffer := e.candleCache[symbol]
	if buffer == nil || buffer.Len() == 0 {
		return nil
	}

	currentCandle := buffer.Last()

	// 0. Volatility Filter (v1.3.0) - reject dead/stable coins early
	// Formula: NDR = (High24h - Low24h) / LastPrice
	// Threshold: 3% minimum volatility
	ticker24h := e.ticker24hStats[symbol]
	if ticker24h != nil && ticker24h.LastPrice > 0 {
		dailyRange := ticker24h.HighPrice24h - ticker24h.LowPrice24h
		volatility := dailyRange / ticker24h.LastPrice
		if volatility < MinVolatility24h {
			log.Debug().
				Str("symbol", symbol).
				Float64("volatility", volatility).
				Float64("threshold", MinVolatility24h).
				Msg("signal rejected: volatility too low (dead coin)")
			return nil // REJECT: Asset is too stable/dead, not worth trading fees
		}
	}

	// 1. Calculate Relative Strength (RS)
	rs, err := e.calculateRS(symbol)
	if err != nil {
		log.Debug().Err(err).Str("symbol", symbol).Msg("RS calculation failed")
		return nil
	}

	// Check weakness threshold
	if rs >= RSWeakThreshold {
		return nil
	}

	// 2. Check funding rate (anti-squeeze filter)
	fundingRate := e.fundingRates[symbol]
	if fundingRate < FundingAntiSqueeze {
		return nil
	}

	// 3. Calculate average volume first (cheap operation)
	avgVolume := e.calculateAvgVolume(buffer)
	if avgVolume == 0 {
		return nil
	}

	volumeRatio := currentCandle.Volume / avgVolume

	// 4. Check volume threshold early
	if volumeRatio < VolumeMinRatio {
		return nil
	}

	// 5. Check close position (no buyback wick)
	candleRange := currentCandle.High - currentCandle.Low
	if candleRange <= 0 {
		return nil
	}
	closePosition := (currentCandle.Close - currentCandle.Low) / candleRange
	if closePosition > ClosePositionMax {
		return nil
	}

	// 6. Detect support level
	support := e.detectSupport(buffer)
	if support == nil {
		return nil
	}

	// 7. Check breakdown condition: Close < Support
	if currentCandle.Close >= support.Price {
		return nil
	}

	// 8. Calculate score
	score := e.calculateScore(rs, volumeRatio, support, currentCandle, fundingRate, closePosition)

	// Check minimum score threshold
	if score < MinScoreForSignal {
		log.Debug().
			Str("symbol", symbol).
			Int("score", score).
			Float64("rs", rs).
			Float64("volume_ratio", volumeRatio).
			Msg("signal score too low, discarded")
		return nil
	}

	return &models.Signal{
		CreatedAt:       time.Now(),
		Symbol:          symbol,
		PriceTrigger:    currentCandle.Close,
		LevelBroken:     support.Price,
		BreakdownVolume: currentCandle.Volume,
		ScoreRS:         rs,
		ScoreTotal:      score,
		Meta: map[string]interface{}{
			"volume_ratio":        volumeRatio,
			"funding_rate":        fundingRate,
			"close_position":      closePosition,
			"support_age_minutes": time.Since(support.Timestamp).Minutes(),
			"support_touch_count": support.TouchCount,
			"avg_volume":          avgVolume,
		},
	}
}

func (e *Engine) calculateRS(symbol string) (float64, error) {
	buffer := e.candleCache[symbol]
	if buffer == nil {
		return 0, fmt.Errorf("no data for symbol")
	}

	assetCount := buffer.Len()
	btcCount := e.btcBuffer.Len()

	if assetCount < MinRSLookback || btcCount < MinRSLookback {
		return 0, fmt.Errorf("insufficient data: asset=%d, btc=%d, need=%d", assetCount, btcCount, MinRSLookback)
	}

	// Adaptive lookback: use available data up to RSLookbackMinutes
	lookback := RSLookbackMinutes
	if assetCount < lookback {
		lookback = assetCount
	}
	if btcCount < lookback {
		lookback = btcCount
	}

	// Get asset price change
	assetOld := buffer.Get(buffer.Len() - lookback).Close
	assetNew := buffer.Last().Close

	if assetOld == 0 {
		return 0, fmt.Errorf("asset old price is zero")
	}
	assetChange := ((assetNew - assetOld) / assetOld) * 100

	// Get BTC price change
	btcOld := e.btcBuffer.Get(e.btcBuffer.Len() - lookback).Close
	btcNew := e.btcBuffer.Last().Close

	if btcOld == 0 {
		return 0, fmt.Errorf("BTC old price is zero")
	}
	btcChange := ((btcNew - btcOld) / btcOld) * 100

	// RS = Asset % Change - BTC % Change
	return assetChange - btcChange, nil
}

func (e *Engine) detectSupport(buffer *RingBuffer) *models.SupportLevel {
	if buffer.Len() < SupportLookback {
		return nil
	}

	recentCandles := buffer.LastN(SupportLookback)

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
	return &fractals[len(fractals)-1]
}

func (e *Engine) calculateAvgVolume(buffer *RingBuffer) float64 {
	window := VolumeAvgWindow
	if buffer.Len() < window {
		window = buffer.Len()
	}

	if window == 0 {
		return 0
	}

	recentCandles := buffer.LastN(window)
	var sum float64
	for _, c := range recentCandles {
		sum += c.Volume
	}

	return sum / float64(len(recentCandles))
}

func (e *Engine) calculateScore(rs float64, volumeRatio float64, support *models.SupportLevel, currentCandle models.Candle, fundingRate float64, closePosition float64) int {
	score := 0

	// Weakness scoring (up to 40 points)
	if rs < RSWeakThreshold {
		score += 30
		if rs < RSVeryWeakThreshold {
			score += 10
		}
	}

	// Volume scoring (up to 30 points) - FIXED: now includes 1.5x-2x range
	if volumeRatio >= VolumeMinRatio {
		score += 10 // Base score for meeting volume threshold
		if volumeRatio >= VolumeMediumRatio {
			score += 10 // Additional for 2x+
			if volumeRatio >= VolumeHighRatio {
				score += 10 // Additional for 3x+
			}
		}
	}

	// Level Age scoring (20 points)
	supportAgeMinutes := time.Since(support.Timestamp).Minutes()
	if supportAgeMinutes > SupportAgeBonus {
		score += 20
	}

	// Funding scoring (20 points) - positive funding supports short breakdowns
	if fundingRate > FundingPositive {
		score += 20
	}

	// Close position scoring (10 points) - very weak close
	if closePosition < ClosePositionStrong {
		score += 10
	}

	// Trend divergence scoring (10 points): BTC green but asset red
	if e.btcBuffer.Len() >= 2 {
		btcPrevClose := e.btcBuffer.Get(e.btcBuffer.Len() - 2).Close
		btcCurrentClose := e.btcBuffer.Last().Close

		if btcPrevClose > 0 {
			btcChange := ((btcCurrentClose - btcPrevClose) / btcPrevClose) * 100

			buffer := e.candleCache[currentCandle.Symbol]
			if buffer != nil && buffer.Len() >= 2 {
				assetPrevClose := buffer.Get(buffer.Len() - 2).Close
				if assetPrevClose > 0 {
					assetChange := ((currentCandle.Close - assetPrevClose) / assetPrevClose) * 100

					// BTC is green (>0%) but asset is red (<0%)
					if btcChange > 0 && assetChange < 0 {
						score += 10
					}
				}
			}
		}
	}

	// Pump Rollover scoring (15 points) - v1.3.0
	// If the coin is up > 5% in the last 24h, but we have a breakdown signal locally,
	// it indicates a potential reversal of a pump ("Hangover" effect). High-quality setup.
	if ticker24h := e.ticker24hStats[currentCandle.Symbol]; ticker24h != nil {
		if ticker24h.Price24hPcnt > PumpRolloverThreshold {
			score += PumpRolloverBonus
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

func (e *Engine) UpdateFundingRates(rates map[string]float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.fundingRates = make(map[string]float64, len(rates))
	for symbol, rate := range rates {
		e.fundingRates[symbol] = rate
	}
}

// UpdateTicker24hStats updates 24h price statistics for volatility filter and pump detection (v1.3.0)
func (e *Engine) UpdateTicker24hStats(stats map[string]*models.Ticker24hStats) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.ticker24hStats = make(map[string]*models.Ticker24hStats, len(stats))
	for symbol, stat := range stats {
		e.ticker24hStats[symbol] = stat
	}

	log.Debug().Int("count", len(stats)).Msg("ticker 24h stats updated")
}

func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"tracked_symbols": len(e.candleCache),
		"btc_candles":     e.btcBuffer.Len(),
	}
}

// GetMarketData returns current market data for a symbol (for debugging)
func (e *Engine) GetMarketData(symbol string) (*models.MarketData, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	buffer, exists := e.candleCache[symbol]
	if !exists || buffer.Len() == 0 {
		return nil, fmt.Errorf("no data for symbol %s", symbol)
	}

	currentCandle := buffer.Last()
	avgVolume := e.calculateAvgVolume(buffer)
	support := e.detectSupport(buffer)

	// Convert ring buffer to slice for MarketData
	candles := buffer.LastN(buffer.Len())

	return &models.MarketData{
		Symbol:        symbol,
		Candles:       candles,
		CurrentPrice:  currentCandle.Close,
		CurrentVolume: currentCandle.Volume,
		AvgVolume:     avgVolume,
		Support:       support,
	}, nil
}

// Helper: Format float with precision (unused but kept for potential future use)
func formatFloat(f float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(f*ratio) / ratio
}
