package strategy

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/islamtagirov/millionaire-bot/internal/utils"
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
	RSWeakThreshold     = -1.2  // Minimum RS to consider asset weak
	RSVeryWeakThreshold = -5.0  // RS threshold for extra weakness points
	VolumeMinRatio      = 1.8   // Minimum volume ratio for signal
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

	// v1.4.1: Anti-false-signal filters (DEPRECATED in v1.8.0 - see DefaultSupportAge)
	// MinSupportAgeMinutes removed - replaced by DefaultSupportAge in v1.8.0

	// v1.5.0: Smart filters (replaces MaxPriceGain24h)
	MaxPriceGain24h = 0.30 // Increased from 0.10 to 0.30 (30%) - allow more pump scenarios

	// v1.5.0: Open Interest Divergence
	OISnapshotAgeMinutes = 15.0  // How old OI snapshot should be for delta calc
	OIDivergenceThreshold = 0.02 // 2% OI change threshold
	OIAggressiveShortBonus = 15  // Bonus: price down + OI up (new shorts entering)
	OILongExitPenalty     = -50  // Penalty: price down + OI down (longs exiting, not shorts)

	// v1.5.0: Volume Z-Score (v1.7.1: now primary volume scoring method)
	VolumeZScoreWindow    = 50  // 24 candles for Z-Score calculation
	VolumeZScoreThreshold = 3.0 // Z-Score threshold for extreme anomaly (+30 total)

	// v1.5.0: Smart Pump Filter
	SmartPumpThreshold    = 0.15 // 15% price gain triggers smart filter
	SmartPumpNearHighDist = 0.03 // 3% from high = still near high (BLOCK)
	SmartPumpRolloverDist = 0.05 // 5% from high = confirmed rollover (STRONG SIGNAL)
	SmartPumpRolloverBonus = 20  // Bonus for confirmed pump rollover with volume

	// v1.8.0: Anti-Bear-Trap Filters
	OversoldThreshold24h       = -0.25 // -25% in 24h = asset already crashed, skip short
	HighVolatilityThreshold1h  = -0.02 // -2% in 1h = high volatility, need stronger support
	HighVolatilitySupportAge   = 45.0  // 45 minutes min support age during high volatility
	DefaultSupportAge          = 30.0  // 30 minutes min support age (normal conditions)
	SMAWindow                  = 200   // 200 candles for SMA calculation
	SMAMinCandles              = 60    // Minimum candles required for meaningful SMA (skip filter if less)
	MAExtensionThreshold       = 0.04  // 4% below SMA200 = extended, skip short (rubber band)
)

// Engine processes candles and generates signals
type Engine struct {
	mu             sync.RWMutex
	candleCache    map[string]*RingBuffer            // symbol -> ring buffer of candles
	btcBuffer      *RingBuffer                       // BTC candles for RS calculation
	fundingRates   map[string]float64                // symbol -> funding rate (percent)
	ticker24hStats map[string]*models.Ticker24hStats // symbol -> 24h price statistics (v1.3.0)
	oiSnapshots    map[string]*models.OISnapshot     // v1.5.0: symbol -> OI snapshot for delta calc
	signalChan     chan *models.Signal
	lastSignal     map[string]time.Time              // symbol -> last signal time (cooldown)
	filterStats    *FilterStats                      // v1.8.1: Pipeline statistics for diagnostics
}

// FilterStats tracks rejection statistics for pipeline diagnostics (v1.8.1)
// Uses atomic counters for lock-free performance
type FilterStats struct {
	CandlesProcessed int64 // Total candles that entered analyzeBreakdown

	// Filter rejection counters (ordered by pipeline position)
	RejectedVolatility    int64 // Filter 1: NDR < 3%
	RejectedOversold24h   int64 // Filter 2: Price24h < -15%
	RejectedMaxPump       int64 // Filter 3: Price24h > 30%
	RejectedSmartPumpNear int64 // Filter 4: Pump > 15% but near high
	RejectedMAExtension   int64 // Filter 5: >4% below SMA200
	RejectedOILongExit    int64 // Filter 6: Price down + OI down
	RejectedRSWeak        int64 // Filter 7: RS >= -3%
	RejectedFunding       int64 // Filter 8: Funding < -0.015%
	RejectedVolume        int64 // Filter 9: Volume < 1.5x
	RejectedClosePosition int64 // Filter 10: Close > 30% of range
	RejectedNoSupport     int64 // Filter 11: No support found
	RejectedSupportAge    int64 // Filter 12: Support too young
	RejectedNoBreakdown   int64 // Close >= Support
	RejectedBounceback    int64 // Wick recovery detected
	RejectedLowScore      int64 // Score < 50
	RejectedCooldown      int64 // 15 min cooldown active

	SignalsGenerated int64 // Successfully emitted signals

	// Near-miss tracking (score 40-49)
	nearMissMu     sync.Mutex
	NearMissSignals []NearMissSignal
}

// NearMissSignal represents a signal that almost passed (score 40-49)
type NearMissSignal struct {
	Symbol    string
	Score     int
	RS        float64
	VolumeZ   float64
	Timestamp time.Time
}

// MaxNearMissSignals limits memory usage for near-miss tracking
const MaxNearMissSignals = 10

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
		oiSnapshots:    make(map[string]*models.OISnapshot), // v1.5.0
		signalChan:     make(chan *models.Signal, 100),
		lastSignal:     make(map[string]time.Time),
		filterStats:    &FilterStats{}, // v1.8.1: Pipeline statistics
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
			if e.filterStats != nil {
				atomic.AddInt64(&e.filterStats.RejectedCooldown, 1)
			}
			return
		}
	}

	select {
	case e.signalChan <- signal:
		e.lastSignal[signal.Symbol] = time.Now()
		if e.filterStats != nil {
			atomic.AddInt64(&e.filterStats.SignalsGenerated, 1)
		}
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

	// v1.8.1: Ensure filterStats is initialized (defensive programming)
	// This should never happen if Engine is created via NewEngine()
	if e.filterStats == nil {
		e.filterStats = &FilterStats{}
	}

	// Increment candles processed counter
	atomic.AddInt64(&e.filterStats.CandlesProcessed, 1)

	currentCandle := buffer.Last()

	// Pre-calculate distFromHigh once (used in multiple places)
	// v1.8.0: Optimization - avoid redundant calculation
	var distFromHigh float64
	ticker24h := e.ticker24hStats[symbol]
	if ticker24h != nil && ticker24h.HighPrice24h > 0 {
		distFromHigh = utils.DistanceFromHigh(currentCandle.Close, ticker24h.HighPrice24h)
	}

	// 0. Volatility Filter (v1.3.0) - reject dead/stable coins early
	// Formula: NDR = (High24h - Low24h) / LastPrice
	// Threshold: 3% minimum volatility
	if ticker24h != nil && ticker24h.LastPrice > 0 {
		dailyRange := ticker24h.HighPrice24h - ticker24h.LowPrice24h
		volatility := dailyRange / ticker24h.LastPrice
		if volatility < MinVolatility24h {
			atomic.AddInt64(&e.filterStats.RejectedVolatility, 1)
			return nil // REJECT: Asset is too stable/dead, not worth trading fees
		}

		// v1.8.0: Oversold Filter (24h Fatigue) - prevent shorting crashed assets
		// If asset already dropped >15% in 24h, it's likely oversold - bear trap risk
		if ticker24h.Price24hPcnt < OversoldThreshold24h {
			atomic.AddInt64(&e.filterStats.RejectedOversold24h, 1)
			return nil
		}

		// v1.5.0: Smart Pump Filter (replaces simple MaxPriceGain24h filter)
		// Allow assets up to 30% gain, but apply smart filtering
		if ticker24h.Price24hPcnt > MaxPriceGain24h {
			atomic.AddInt64(&e.filterStats.RejectedMaxPump, 1)
			return nil
		}

		// v1.5.0: Smart Pump Filter - if pump > 15%, check distance from high
		if ticker24h.Price24hPcnt > SmartPumpThreshold && ticker24h.HighPrice24h > 0 {
			// BLOCK: If pump > 15% AND price still near high (< 3% drop) - knife catching
			if distFromHigh < SmartPumpNearHighDist {
				atomic.AddInt64(&e.filterStats.RejectedSmartPumpNear, 1)
				return nil
			}
		}
	}

	// v1.8.0: MA Extension Filter (Rubber Band Effect) - prevent shorting too extended assets
	// If price is >4% below SMA200, mean reversion risk is high
	sma200, smaAvailable := e.calculateSMA200(buffer)
	if smaAvailable {
		deviation := utils.CalculateDeviation(currentCandle.Close, sma200)
		if deviation > MAExtensionThreshold {
			atomic.AddInt64(&e.filterStats.RejectedMAExtension, 1)
			return nil
		}
	}

	// v1.5.0: Calculate OI divergence early for potential block
	deltaOI, isAggressiveShort, isLongExit := e.calculateOIDivergence(symbol, currentCandle)
	if isLongExit {
		atomic.AddInt64(&e.filterStats.RejectedOILongExit, 1)
		return nil
	}

	// 1. Calculate Relative Strength (RS)
	rs, err := e.calculateRS(symbol)
	if err != nil {
		atomic.AddInt64(&e.filterStats.RejectedRSWeak, 1)
		return nil
	}

	// Check weakness threshold
	if rs >= RSWeakThreshold {
		atomic.AddInt64(&e.filterStats.RejectedRSWeak, 1)
		return nil
	}

	// 2. Check funding rate (anti-squeeze filter)
	fundingRate := e.fundingRates[symbol]
	if fundingRate < FundingAntiSqueeze {
		atomic.AddInt64(&e.filterStats.RejectedFunding, 1)
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
		atomic.AddInt64(&e.filterStats.RejectedVolume, 1)
		return nil
	}

	// 5. Check close position (no buyback wick)
	candleRange := currentCandle.High - currentCandle.Low
	if candleRange <= 0 {
		return nil
	}
	closePosition := (currentCandle.Close - currentCandle.Low) / candleRange
	if closePosition > ClosePositionMax {
		atomic.AddInt64(&e.filterStats.RejectedClosePosition, 1)
		return nil
	}

	// 6. Detect support level (v1.4.0: consolidation detection with fractal fallback)
	candles := buffer.LastN(buffer.Len())
	support := DetectSupport(candles)
	if support == nil {
		atomic.AddInt64(&e.filterStats.RejectedNoSupport, 1)
		return nil
	}

	// 6.1 v1.8.0: Dynamic Support Age (replaces v1.4.1 static MinSupportAgeMinutes)
	// During high volatility (price dropped >2% in 1h), require stronger/older support levels
	supportAgeMinutes := time.Since(support.Timestamp).Minutes()
	minSupportAge := DefaultSupportAge // Default 15 minutes

	// Calculate 1h price change from candle buffer
	change1h := e.calculateChange1h(buffer)
	if change1h < HighVolatilityThreshold1h {
		// High volatility detected - asset dropped >2% in last hour
		// Require older support levels (45 min) to avoid temporary pauses
		minSupportAge = HighVolatilitySupportAge
	}

	if supportAgeMinutes < minSupportAge {
		atomic.AddInt64(&e.filterStats.RejectedSupportAge, 1)
		return nil
	}

	// 7. Check breakdown condition: Close < Support
	if currentCandle.Close >= support.Price {
		atomic.AddInt64(&e.filterStats.RejectedNoBreakdown, 1)
		return nil
	}

	// 8. Confirm no bounceback (v1.4.0)
	if !ConfirmNoBounceback(candles, support.Price) {
		atomic.AddInt64(&e.filterStats.RejectedBounceback, 1)
		return nil
	}

	// 9. Calculate Volume Z-Score (v1.5.0)
	volumeZScore, meanVol, stdDevVol := e.calculateVolumeZScore(buffer, currentCandle.Volume)

	// 10. Check for Smart Pump Rollover bonus eligibility (v1.5.0)
	// distFromHigh already calculated at the top of analyzeBreakdown (v1.8.0 optimization)
	isSmartPumpRollover := false
	if ticker24h != nil && ticker24h.HighPrice24h > 0 {
		// Smart Pump Rollover: pump > 15%, distance from high > 5%, Z-Score > 3.0
		if ticker24h.Price24hPcnt > SmartPumpThreshold &&
			distFromHigh > SmartPumpRolloverDist &&
			volumeZScore > VolumeZScoreThreshold {
			isSmartPumpRollover = true
		}
	}

	// 11. Calculate score with new v1.5.0 factors
	score := e.calculateScoreV150(
		rs, volumeRatio, support, currentCandle, fundingRate, closePosition,
		volumeZScore, deltaOI, isAggressiveShort, isSmartPumpRollover,
	)

	// Check minimum score threshold
	if score < MinScoreForSignal {
		atomic.AddInt64(&e.filterStats.RejectedLowScore, 1)
		// v1.8.1: Track near-miss signals (score 40-49) for diagnostics
		if score >= 40 {
			e.recordNearMiss(symbol, score, rs, volumeZScore)
		}
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
			"volume_ratio":           volumeRatio,
			"funding_rate":           fundingRate,
			"close_position":         closePosition,
			"support_age_minutes":    time.Since(support.Timestamp).Minutes(),
			"support_touch_count":    support.TouchCount,
			"is_consolidation_break": support.IsConsolidation,
			"avg_volume":             avgVolume,
			// v1.5.0 metrics
			"volume_z_score":       volumeZScore,
			"volume_mean":          meanVol,
			"volume_stddev":        stdDevVol,
			"oi_delta_pct":         deltaOI * 100, // Store as percentage
			"is_aggressive_short":  isAggressiveShort,
			"is_smart_pump_rollover": isSmartPumpRollover,
			"dist_from_high_pct":   distFromHigh * 100,
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

// detectSupport removed in v1.4.0 - now using DetectSupport() from support_detector.go
// which includes consolidation detection with fractal fallback

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

	// Pump Rollover scoring (15 points) - v1.3.0, FIXED in v1.4.1
	// If the coin is up > 5% in the last 24h AND current candle is red (confirms reversal),
	// it indicates a potential reversal of a pump ("Hangover" effect). High-quality setup.
	if ticker24h := e.ticker24hStats[currentCandle.Symbol]; ticker24h != nil {
		// Only give bonus if: 1) Asset was pumping 2) Current candle is RED (confirms reversal)
		isCurrentCandleRed := currentCandle.Close < currentCandle.Open
		if ticker24h.Price24hPcnt > PumpRolloverThreshold && isCurrentCandleRed {
			score += PumpRolloverBonus
		}
	}

	// Support touch count bonus (up to 15 points) - v1.4.0
	// Multiple touches of support level = stronger level
	if support.TouchCount >= 3 {
		score += 15
	} else if support.TouchCount >= 2 {
		score += 10
	}

	// Consolidation breakout bonus (10 points) - v1.4.0
	// Consolidation breakdowns are stronger than simple fractal low breakdowns
	if support.IsConsolidation {
		score += 10
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	return score
}

// calculateScoreV150 is the enhanced scoring function with v1.5.0 features
// Includes: Volume Z-Score, OI Divergence, Smart Pump Rollover
func (e *Engine) calculateScoreV150(
	rs float64,
	volumeRatio float64,
	support *models.SupportLevel,
	currentCandle models.Candle,
	fundingRate float64,
	closePosition float64,
	volumeZScore float64,
	deltaOI float64,
	isAggressiveShort bool,
	isSmartPumpRollover bool,
) int {
	score := 0

	// Weakness scoring (up to 40 points)
	if rs < RSWeakThreshold {
		score += 30
		if rs < RSVeryWeakThreshold {
			score += 10
		}
	}

	// v1.7.1: Volume scoring using Z-Score only (removed legacy ratio scoring to avoid duplication)
	// Z-Score is statistically more accurate as it accounts for variance
	// Scale: Z>2.0 = +10, Z>2.5 = +20, Z>3.0 = +30
	if volumeZScore > 2.0 {
		score += 10 // Moderate anomaly
		if volumeZScore > 2.5 {
			score += 10 // Strong anomaly
			if volumeZScore > VolumeZScoreThreshold { // 3.0
				score += 10 // Extreme anomaly
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

	// v1.5.0: OI Divergence scoring
	// Aggressive short: price down + OI up = new shorts entering (bullish for short position)
	if isAggressiveShort {
		score += OIAggressiveShortBonus
	}

	// v1.5.0: Smart Pump Rollover (replaces old Pump Rollover)
	// Pump > 15%, distance from high > 5%, Z-Score > 3.0 = confirmed reversal
	if isSmartPumpRollover {
		score += SmartPumpRolloverBonus
	} else {
		// Fallback to legacy Pump Rollover logic for non-pump scenarios
		if ticker24h := e.ticker24hStats[currentCandle.Symbol]; ticker24h != nil {
			isCurrentCandleRed := currentCandle.Close < currentCandle.Open
			if ticker24h.Price24hPcnt > PumpRolloverThreshold && isCurrentCandleRed {
				// Only give smaller bonus if not already qualified for smart pump rollover
				score += 10 // Reduced from 15 to 10 for legacy pump rollover
			}
		}
	}

	// Support touch count bonus (up to 15 points) - v1.4.0
	// Multiple touches of support level = stronger level
	if support.TouchCount >= 3 {
		score += 15
	} else if support.TouchCount >= 2 {
		score += 10
	}

	// Consolidation breakout bonus (10 points) - v1.4.0
	// Consolidation breakdowns are stronger than simple fractal low breakdowns
	if support.IsConsolidation {
		score += 10
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
// v1.5.0: Also initializes OI snapshots for divergence analysis
func (e *Engine) UpdateTicker24hStats(stats map[string]*models.Ticker24hStats) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	// Track which symbols are still active (for cleanup)
	activeSymbols := make(map[string]struct{}, len(stats))

	// Update ticker stats
	e.ticker24hStats = make(map[string]*models.Ticker24hStats, len(stats))
	for symbol, stat := range stats {
		e.ticker24hStats[symbol] = stat
		activeSymbols[symbol] = struct{}{}

		// v1.5.0: Initialize OI snapshots for new symbols only
		// The actual snapshot UPDATE happens in calculateOIDivergence AFTER comparison
		// This ensures we always compare current OI against 15+ minute old baseline
		if stat.OpenInterest > 0 {
			if _, exists := e.oiSnapshots[symbol]; !exists {
				// First time seeing this symbol - create baseline snapshot
				e.oiSnapshots[symbol] = &models.OISnapshot{
					OpenInterest: stat.OpenInterest,
					Timestamp:    now,
				}
			}
			// Existing snapshots are updated in calculateOIDivergence after comparison
		}
	}

	// v1.5.0: Cleanup old snapshots for symbols no longer tracked (prevent memory leak)
	for symbol := range e.oiSnapshots {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.oiSnapshots, symbol)
		}
	}

	log.Debug().
		Int("stats_count", len(stats)).
		Int("oi_snapshots", len(e.oiSnapshots)).
		Msg("ticker 24h stats and OI snapshots updated")
}

// calculateOIDivergence calculates Open Interest divergence (v1.5.0)
// Returns: deltaOI (percentage change), isAggressiveShort (OI up while price down), isLongExit (OI down while price down)
// IMPORTANT: This function updates the snapshot AFTER comparison to ensure proper timing
// v1.7.0: Fixed potential memory leak - only updates snapshot if symbol is actively tracked
func (e *Engine) calculateOIDivergence(symbol string, currentCandle models.Candle) (deltaOI float64, isAggressiveShort bool, isLongExit bool) {
	ticker := e.ticker24hStats[symbol]
	snapshot := e.oiSnapshots[symbol]

	// Need both current OI and historical snapshot
	if ticker == nil || snapshot == nil || ticker.OpenInterest <= 0 || snapshot.OpenInterest <= 0 {
		return 0, false, false
	}

	snapshotAge := time.Since(snapshot.Timestamp).Minutes()

	// Check if snapshot is old enough for meaningful delta
	if snapshotAge < OISnapshotAgeMinutes {
		return 0, false, false
	}

	// Calculate OI change: compare CURRENT OI (from ticker) with HISTORICAL OI (from snapshot)
	deltaOI = (ticker.OpenInterest - snapshot.OpenInterest) / snapshot.OpenInterest

	// v1.7.0 FIX: Only update snapshot if symbol is still in ticker24hStats
	// This prevents memory leaks from orphaned snapshots
	if _, stillTracked := e.ticker24hStats[symbol]; stillTracked {
		e.oiSnapshots[symbol] = &models.OISnapshot{
			OpenInterest: ticker.OpenInterest,
			Timestamp:    time.Now(),
		}
	}

	// Check if current candle is bearish (price going down)
	isPriceDown := currentCandle.Close < currentCandle.Open

	if isPriceDown {
		if deltaOI > OIDivergenceThreshold {
			// Price down + OI up = New shorts entering = Aggressive short signal
			isAggressiveShort = true
		} else if deltaOI < -OIDivergenceThreshold {
			// Price down + OI down = Longs exiting = Not a short opportunity (squeeze risk)
			isLongExit = true
		}
	}

	return deltaOI, isAggressiveShort, isLongExit
}

// calculateVolumeZScore calculates Volume Z-Score using utils package (v1.5.0)
// Optimized: reuses pre-allocated slice to reduce GC pressure
func (e *Engine) calculateVolumeZScore(buffer *RingBuffer, currentVolume float64) (zScore float64, meanVol float64, stdDevVol float64) {
	// Need at least 3 candles: 2 for history + 1 current
	if buffer.Len() < 3 {
		return 0, 0, 0
	}

	window := VolumeZScoreWindow
	if buffer.Len() < window {
		window = buffer.Len()
	}

	// Calculate mean and stddev directly from buffer to avoid slice allocation
	// We exclude the last candle (current) from historical data
	historyLen := window - 1
	if historyLen < 2 {
		return 0, 0, 0
	}

	// Calculate mean directly from ring buffer
	var sum float64
	startIdx := buffer.Len() - window
	if startIdx < 0 {
		startIdx = 0
	}
	for i := startIdx; i < buffer.Len()-1; i++ { // Exclude last (current) candle
		sum += buffer.Get(i).Volume
	}
	meanVol = sum / float64(historyLen)

	// Calculate stddev
	var sumSquares float64
	for i := startIdx; i < buffer.Len()-1; i++ {
		diff := buffer.Get(i).Volume - meanVol
		sumSquares += diff * diff
	}
	stdDevVol = math.Sqrt(sumSquares / float64(historyLen-1)) // Sample stddev

	if stdDevVol == 0 {
		return 0, meanVol, 0
	}

	zScore = (currentVolume - meanVol) / stdDevVol
	return zScore, meanVol, stdDevVol
}

// calculateSMA200 calculates Simple Moving Average from last 200 closing prices (v1.8.0)
// Returns SMA value and boolean indicating if calculation was possible
// Uses allocation-free direct buffer access for performance
// IMPORTANT: Returns available=false if we have fewer than SMAMinCandles (60) to avoid
// unreliable SMA calculations. Short-period SMA is much more volatile than SMA200.
func (e *Engine) calculateSMA200(buffer *RingBuffer) (sma float64, available bool) {
	// Need minimum candles for meaningful SMA calculation
	// With fewer candles, SMA becomes too volatile and filter gives false positives
	if buffer.Len() < SMAMinCandles {
		return 0, false // Not enough data for reliable SMA
	}

	window := SMAWindow
	if buffer.Len() < window {
		window = buffer.Len()
	}

	// Calculate SMA directly from ring buffer to avoid slice allocation
	var sum float64
	startIdx := buffer.Len() - window
	for i := startIdx; i < buffer.Len(); i++ {
		sum += buffer.Get(i).Close
	}

	return sum / float64(window), true
}

// calculateChange1h calculates price change over last 60 minutes (v1.8.0)
// Returns change as decimal (e.g., -0.02 = -2%)
// Used for Dynamic Support Age filter to detect high volatility
func (e *Engine) calculateChange1h(buffer *RingBuffer) float64 {
	// Need at least 60 candles for 1h lookback (1-min candles)
	lookback := 60
	if buffer.Len() < lookback {
		lookback = buffer.Len()
	}
	if lookback < 2 {
		return 0
	}

	oldPrice := buffer.Get(buffer.Len() - lookback).Close
	newPrice := buffer.Last().Close

	if oldPrice == 0 {
		return 0
	}

	return (newPrice - oldPrice) / oldPrice
}

func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"tracked_symbols": len(e.candleCache),
		"btc_candles":     e.btcBuffer.Len(),
		"oi_snapshots":    len(e.oiSnapshots),
		"last_signals":    len(e.lastSignal),
	}
}

// recordNearMiss saves a near-miss signal (score 40-49) for diagnostics (v1.8.1)
func (e *Engine) recordNearMiss(symbol string, score int, rs float64, volumeZ float64) {
	if e.filterStats == nil {
		return // Safety check
	}

	e.filterStats.nearMissMu.Lock()
	defer e.filterStats.nearMissMu.Unlock()

	// Limit memory usage - use copy to avoid memory leak from slice re-slicing
	// (re-slicing keeps underlying array allocated)
	if len(e.filterStats.NearMissSignals) >= MaxNearMissSignals {
		// Shift elements left and reuse underlying array
		copy(e.filterStats.NearMissSignals, e.filterStats.NearMissSignals[1:])
		e.filterStats.NearMissSignals = e.filterStats.NearMissSignals[:MaxNearMissSignals-1]
	}

	e.filterStats.NearMissSignals = append(e.filterStats.NearMissSignals, NearMissSignal{
		Symbol:    symbol,
		Score:     score,
		RS:        rs,
		VolumeZ:   volumeZ,
		Timestamp: time.Now(),
	})
}

// GetFilterStats returns a snapshot of filter statistics (v1.8.1)
// Returns aggregated rejection counts and near-miss signals
// Thread-safe: takes mutex to ensure consistent snapshot
func (e *Engine) GetFilterStats() map[string]interface{} {
	if e.filterStats == nil {
		return map[string]interface{}{} // Safety check
	}

	// Lock near-miss mutex FIRST to ensure consistent snapshot
	// (atomic counters are read atomically, but we need consistency with near-miss)
	e.filterStats.nearMissMu.Lock()
	defer e.filterStats.nearMissMu.Unlock()

	// Copy near-miss signals while holding lock
	nearMiss := make([]map[string]interface{}, 0, len(e.filterStats.NearMissSignals))
	for _, nm := range e.filterStats.NearMissSignals {
		nearMiss = append(nearMiss, map[string]interface{}{
			"symbol":   nm.Symbol,
			"score":    nm.Score,
			"rs":       nm.RS,
			"volume_z": nm.VolumeZ,
		})
	}

	// Read atomic counters (lock-free, but done after near-miss copy for logical consistency)
	stats := map[string]interface{}{
		"candles_processed": atomic.LoadInt64(&e.filterStats.CandlesProcessed),
		"signals_generated": atomic.LoadInt64(&e.filterStats.SignalsGenerated),
		"rejections": map[string]int64{
			"volatility":      atomic.LoadInt64(&e.filterStats.RejectedVolatility),
			"oversold_24h":    atomic.LoadInt64(&e.filterStats.RejectedOversold24h),
			"max_pump":        atomic.LoadInt64(&e.filterStats.RejectedMaxPump),
			"smart_pump_near": atomic.LoadInt64(&e.filterStats.RejectedSmartPumpNear),
			"ma_extension":    atomic.LoadInt64(&e.filterStats.RejectedMAExtension),
			"oi_long_exit":    atomic.LoadInt64(&e.filterStats.RejectedOILongExit),
			"rs_weak":         atomic.LoadInt64(&e.filterStats.RejectedRSWeak),
			"funding":         atomic.LoadInt64(&e.filterStats.RejectedFunding),
			"volume":          atomic.LoadInt64(&e.filterStats.RejectedVolume),
			"close_position":  atomic.LoadInt64(&e.filterStats.RejectedClosePosition),
			"no_support":      atomic.LoadInt64(&e.filterStats.RejectedNoSupport),
			"support_age":     atomic.LoadInt64(&e.filterStats.RejectedSupportAge),
			"no_breakdown":    atomic.LoadInt64(&e.filterStats.RejectedNoBreakdown),
			"bounceback":      atomic.LoadInt64(&e.filterStats.RejectedBounceback),
			"low_score":       atomic.LoadInt64(&e.filterStats.RejectedLowScore),
			"cooldown":        atomic.LoadInt64(&e.filterStats.RejectedCooldown),
		},
		"near_miss":       nearMiss,
		"near_miss_count": len(nearMiss),
	}

	return stats
}

// ResetFilterStats clears all counters for the next period (v1.8.1)
// Thread-safe: uses atomic stores and mutex for near-miss
func (e *Engine) ResetFilterStats() {
	if e.filterStats == nil {
		return // Safety check
	}

	// Lock near-miss mutex FIRST (same order as GetFilterStats to prevent deadlock)
	e.filterStats.nearMissMu.Lock()
	
	// Reset atomic counters while holding lock (ensures consistency with GetFilterStats)
	atomic.StoreInt64(&e.filterStats.CandlesProcessed, 0)
	atomic.StoreInt64(&e.filterStats.SignalsGenerated, 0)
	atomic.StoreInt64(&e.filterStats.RejectedVolatility, 0)
	atomic.StoreInt64(&e.filterStats.RejectedOversold24h, 0)
	atomic.StoreInt64(&e.filterStats.RejectedMaxPump, 0)
	atomic.StoreInt64(&e.filterStats.RejectedSmartPumpNear, 0)
	atomic.StoreInt64(&e.filterStats.RejectedMAExtension, 0)
	atomic.StoreInt64(&e.filterStats.RejectedOILongExit, 0)
	atomic.StoreInt64(&e.filterStats.RejectedRSWeak, 0)
	atomic.StoreInt64(&e.filterStats.RejectedFunding, 0)
	atomic.StoreInt64(&e.filterStats.RejectedVolume, 0)
	atomic.StoreInt64(&e.filterStats.RejectedClosePosition, 0)
	atomic.StoreInt64(&e.filterStats.RejectedNoSupport, 0)
	atomic.StoreInt64(&e.filterStats.RejectedSupportAge, 0)
	atomic.StoreInt64(&e.filterStats.RejectedNoBreakdown, 0)
	atomic.StoreInt64(&e.filterStats.RejectedBounceback, 0)
	atomic.StoreInt64(&e.filterStats.RejectedLowScore, 0)
	atomic.StoreInt64(&e.filterStats.RejectedCooldown, 0)

	// Clear near-miss signals (reuse underlying array to avoid allocation)
	e.filterStats.NearMissSignals = e.filterStats.NearMissSignals[:0]
	
	e.filterStats.nearMissMu.Unlock()
}

// CleanupInactiveSymbols removes data for symbols that are no longer actively tracked
// v1.7.0: Fixes memory leaks in candleCache, lastSignal, and oiSnapshots
// activeSymbols is a set of symbols currently subscribed via WebSocket
// Returns the number of symbols cleaned up
func (e *Engine) CleanupInactiveSymbols(activeSymbols map[string]struct{}) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	cleaned := 0

	// Cleanup candleCache - remove symbols not in active set
	for symbol := range e.candleCache {
		if symbol == "BTCUSDT" {
			continue // Never remove BTC - required for RS calculation
		}
		if _, active := activeSymbols[symbol]; !active {
			delete(e.candleCache, symbol)
			cleaned++
		}
	}

	// Cleanup lastSignal - remove old entries for inactive symbols
	for symbol := range e.lastSignal {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.lastSignal, symbol)
		}
	}

	// Cleanup oiSnapshots - remove inactive symbols
	// (ticker24hStats is already cleaned in UpdateTicker24hStats)
	for symbol := range e.oiSnapshots {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.oiSnapshots, symbol)
		}
	}

	if cleaned > 0 {
		log.Debug().
			Int("cleaned_candle_caches", cleaned).
			Int("remaining_symbols", len(e.candleCache)).
			Msg("cleaned up inactive symbol data from engine")
	}

	return cleaned
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

	// Convert ring buffer to slice for MarketData
	candles := buffer.LastN(buffer.Len())

	// Use new support detection (v1.4.0)
	support := DetectSupport(candles)

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
