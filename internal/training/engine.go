package training

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/db"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/islamtagirov/millionaire-bot/internal/strategy"
	"github.com/islamtagirov/millionaire-bot/internal/utils"
	"github.com/rs/zerolog/log"
)

// DataMiningEngine processes candles and saves training data
// Key difference from strategy.Engine: Structure detection BEFORE quality filters
// INVERTED LOGIC (v1.9.0):
// 1. Detect Support (structure)
// 2. Check Breakdown (event)
// 3. SAVE TO DB (all breakdowns!)
// 4. Quality filters (RS, Volume, Funding...)
// 5. Mark is_shadow_mode accordingly
type DataMiningEngine struct {
	mu             sync.RWMutex
	candleCache    map[string]*strategy.RingBuffer            // Reuse RingBuffer from strategy
	btcBuffer      *strategy.RingBuffer                       // BTC for RS calculation
	fundingRates   map[string]float64                         // symbol -> funding rate (percent)
	ticker24hStats map[string]*models.Ticker24hStats          // symbol -> 24h stats
	oiSnapshots    map[string]*models.OISnapshot              // symbol -> OI snapshot
	store          *db.Store                                  // Database store
	signalChan     chan *models.Signal                        // For elite signals that pass all filters
	lastSignal     map[string]time.Time                       // Cooldown for Telegram signals
}

func NewDataMiningEngine(store *db.Store) *DataMiningEngine {
	return &DataMiningEngine{
		candleCache:    make(map[string]*strategy.RingBuffer),
		btcBuffer:      strategy.NewRingBuffer(MaxCandlesInMemory),
		fundingRates:   make(map[string]float64),
		ticker24hStats: make(map[string]*models.Ticker24hStats),
		oiSnapshots:    make(map[string]*models.OISnapshot),
		store:          store,
		signalChan:     make(chan *models.Signal, 100),
		lastSignal:     make(map[string]time.Time),
	}
}

func (e *DataMiningEngine) ProcessCandle(candle models.Candle) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Store BTC candles separately
	if candle.Symbol == "BTCUSDT" {
		e.btcBuffer.Push(candle)
		return
	}

	// Store candle in symbol-specific buffer
	if _, exists := e.candleCache[candle.Symbol]; !exists {
		e.candleCache[candle.Symbol] = strategy.NewRingBuffer(MaxCandlesInMemory)
	}
	e.candleCache[candle.Symbol].Push(candle)

	// Check if we have enough data for analysis
	assetCount := e.candleCache[candle.Symbol].Len()
	btcCount := e.btcBuffer.Len()

	if assetCount < MinRSLookback || btcCount < MinRSLookback {
		return
	}

	// Analyze for breakdown (STRUCTURE FIRST!)
	e.analyzeBreakdownForTraining(candle.Symbol)
}

// analyzeBreakdownForTraining implements INVERTED LOGIC (v1.9.0)
// Step 1: Find STRUCTURE (Support + Breakdown)
// Step 2: SAVE to training_data
// Step 3: Apply QUALITY filters to determine is_shadow_mode
func (e *DataMiningEngine) analyzeBreakdownForTraining(symbol string) {
	buffer := e.candleCache[symbol]
	if buffer == nil || buffer.Len() == 0 {
		return
	}

	currentCandle := buffer.Last()

	// ==============================================
	// PHASE 1: STRUCTURE DETECTION (Event-first!)
	// ==============================================

	// 1. Detect support level
	candles := buffer.LastN(buffer.Len())
	support := strategy.DetectSupport(candles)
	if support == nil {
		return // No support = no breakdown possible
	}

	// 2. Check breakdown condition: Close < Support
	if currentCandle.Close >= support.Price {
		return // No breakdown = nothing to save
	}

	// 3. Confirm no bounceback
	if !strategy.ConfirmNoBounceback(candles, support.Price) {
		return // Bounceback detected = false breakdown
	}

	// ==============================================
	// BREAKDOWN EVENT DETECTED!
	// Now we save this to training_data regardless of quality
	// ==============================================

	log.Info().
		Str("symbol", symbol).
		Float64("price", currentCandle.Close).
		Float64("support", support.Price).
		Msg("📊 BREAKDOWN DETECTED - capturing training data")

	// ==============================================
	// PHASE 2: CALCULATE ALL FEATURES
	// ==============================================

	features := e.calculateFeatures(symbol, currentCandle, support, buffer)
	if features == nil {
		return // Failed to calculate features
	}

	// ==============================================
	// PHASE 3: APPLY QUALITY FILTERS
	// Check if this breakdown would pass trading filters
	// ==============================================

	isShadowMode := !e.passesQualityFilters(symbol, currentCandle, support, buffer, features)

	// ==============================================
	// PHASE 4: SAVE TO DATABASE
	// ==============================================

	trainingData := &models.TrainingData{
		Symbol:             symbol,
		EntryPrice:         currentCandle.Close,
		EntrySupportLevel:  support.Price,
		Features:           features,
		IsShadowMode:       isShadowMode,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.SaveTrainingData(ctx, trainingData); err != nil {
		log.Error().Err(err).Str("symbol", symbol).Msg("failed to save training data")
		return
	}

	// ==============================================
	// PHASE 5: EMIT ELITE SIGNALS TO TELEGRAM
	// Only if passed ALL quality filters
	// ==============================================

	if !isShadowMode {
		// This is an elite signal - also send to Telegram
		score := e.calculateScore(features)

		signal := &models.Signal{
			Symbol:          symbol,
			PriceTrigger:    currentCandle.Close,
			LevelBroken:     support.Price,
			BreakdownVolume: currentCandle.Volume,
			ScoreRS:         features["rs"].(float64),
			ScoreTotal:      score,
			Meta:            features, // Reuse features as meta
		}

		e.emitSignal(signal)
	}
}

// calculateFeatures extracts ALL indicators for ML model (v1.9.0)
// Returns JSONB-compatible map with all features
func (e *DataMiningEngine) calculateFeatures(
	symbol string,
	currentCandle models.Candle,
	support *models.SupportLevel,
	buffer *strategy.RingBuffer,
) map[string]interface{} {

	features := make(map[string]interface{})

	// 1. Relative Strength (RS)
	rs, err := e.calculateRS(symbol)
	if err != nil {
		return nil // Critical feature - fail if not available
	}
	features["rs"] = rs

	// 2. Volume metrics
	avgVolume := e.calculateAvgVolume(buffer)
	if avgVolume == 0 {
		return nil
	}
	volumeRatio := currentCandle.Volume / avgVolume
	volumeZScore, meanVol, stdDevVol := e.calculateVolumeZScore(buffer, currentCandle.Volume)

	features["volume_ratio"] = volumeRatio
	features["volume_z_score"] = volumeZScore
	features["volume_mean"] = meanVol
	features["volume_stddev"] = stdDevVol
	features["avg_volume"] = avgVolume

	// 3. Funding rate
	fundingRate := e.fundingRates[symbol]
	features["funding_rate"] = fundingRate

	// 4. Close position (wick analysis)
	candleRange := currentCandle.High - currentCandle.Low
	if candleRange <= 0 {
		return nil
	}
	closePosition := (currentCandle.Close - currentCandle.Low) / candleRange
	features["close_position"] = closePosition

	// 5. Support level metrics
	supportAgeMinutes := time.Since(support.Timestamp).Minutes()
	features["support_age_minutes"] = supportAgeMinutes
	features["support_touch_count"] = support.TouchCount
	features["is_consolidation_break"] = support.IsConsolidation

	// 6. 24h statistics
	ticker24h := e.ticker24hStats[symbol]
	if ticker24h != nil {
		// Volatility (NDR)
		if ticker24h.LastPrice > 0 {
			dailyRange := ticker24h.HighPrice24h - ticker24h.LowPrice24h
			ndr := dailyRange / ticker24h.LastPrice
			features["ndr_24h"] = ndr
		} else {
			features["ndr_24h"] = 0.0
		}

		features["price_24h_pcnt"] = ticker24h.Price24hPcnt
		features["high_price_24h"] = ticker24h.HighPrice24h
		features["low_price_24h"] = ticker24h.LowPrice24h

		// Distance from high
		if ticker24h.HighPrice24h > 0 {
			distFromHigh := utils.DistanceFromHigh(currentCandle.Close, ticker24h.HighPrice24h)
			features["dist_from_high_pct"] = distFromHigh * 100
		} else {
			features["dist_from_high_pct"] = 0.0
		}
	}

	// 7. OI Divergence
	deltaOI, isAggressiveShort, isLongExit := e.calculateOIDivergence(symbol, currentCandle)
	features["oi_delta_pct"] = deltaOI * 100
	features["is_aggressive_short"] = isAggressiveShort
	features["is_long_exit"] = isLongExit

	// 8. MA200 deviation
	sma200, smaAvailable := e.calculateSMA200(buffer)
	if smaAvailable {
		deviation := utils.CalculateDeviation(currentCandle.Close, sma200)
		features["sma200_deviation"] = deviation
	} else {
		features["sma200_deviation"] = 0.0
	}

	// 9. Change1h (for volatility detection)
	change1h := e.calculateChange1h(buffer)
	features["change_1h"] = change1h

	// 10. BTC divergence
	isBTCDivergence := false
	if e.btcBuffer.Len() >= 2 {
		btcPrevClose := e.btcBuffer.Get(e.btcBuffer.Len() - 2).Close
		btcCurrentClose := e.btcBuffer.Last().Close
		if btcPrevClose > 0 {
			btcChange := ((btcCurrentClose - btcPrevClose) / btcPrevClose) * 100
			if buffer.Len() >= 2 {
				assetPrevClose := buffer.Get(buffer.Len() - 2).Close
				if assetPrevClose > 0 {
					assetChange := ((currentCandle.Close - assetPrevClose) / assetPrevClose) * 100
					// BTC green, asset red
					if btcChange > 0 && assetChange < 0 {
						isBTCDivergence = true
					}
				}
			}
		}
	}
	features["btc_divergence"] = isBTCDivergence

	// 11. Smart Pump Rollover detection
	isSmartPumpRollover := false
	if ticker24h != nil && ticker24h.HighPrice24h > 0 {
		distFromHigh := utils.DistanceFromHigh(currentCandle.Close, ticker24h.HighPrice24h)
		if ticker24h.Price24hPcnt > SmartPumpThreshold &&
			distFromHigh > SmartPumpRolloverDist &&
			volumeZScore > VolumeZScoreThreshold {
			isSmartPumpRollover = true
		}
	}
	features["is_smart_pump_rollover"] = isSmartPumpRollover

	// 12. Current candle is red?
	isCurrentCandleRed := currentCandle.Close < currentCandle.Open
	features["is_red_candle"] = isCurrentCandleRed

	// 13. Time of Day (hour of day, 0-23)
	hourOfDay := currentCandle.Timestamp.UTC().Hour()
	features["hour_of_day"] = hourOfDay

	// 14. Day of Week (0=Sunday, 6=Saturday)
	dayOfWeek := int(currentCandle.Timestamp.UTC().Weekday())
	features["day_of_week"] = dayOfWeek

	// 15. Candle properties
	features["candle_high"] = currentCandle.High
	features["candle_low"] = currentCandle.Low
	features["candle_open"] = currentCandle.Open
	features["candle_close"] = currentCandle.Close
	features["candle_volume"] = currentCandle.Volume
	features["candle_range"] = candleRange

	return features
}

// passesQualityFilters checks if breakdown passes trading-grade filters (v1.9.0)
// Returns TRUE if signal is "elite" (would be sent to Telegram)
// Returns FALSE if signal is "shadow" (learning data only)
func (e *DataMiningEngine) passesQualityFilters(
	symbol string,
	currentCandle models.Candle,
	support *models.SupportLevel,
	buffer *strategy.RingBuffer,
	features map[string]interface{},
) bool {

	// Extract features
	rs := features["rs"].(float64)
	volumeRatio := features["volume_ratio"].(float64)
	fundingRate := features["funding_rate"].(float64)
	closePosition := features["close_position"].(float64)
	ndr := features["ndr_24h"].(float64)

	ticker24h := e.ticker24hStats[symbol]

	// Filter 1: Volatility (NDR < 3%)
	if ndr < MinVolatility24h {
		return false
	}

	// Filter 2: Oversold 24h (< -40% for data mining, vs -25% for trading)
	if ticker24h != nil && ticker24h.Price24hPcnt < OversoldThreshold24h {
		return false
	}

	// Filter 3: Max Pump (> 30%)
	if ticker24h != nil && ticker24h.Price24hPcnt > MaxPriceGain24h {
		return false
	}

	// Filter 4: Smart Pump Near High
	if ticker24h != nil && ticker24h.HighPrice24h > 0 {
		if ticker24h.Price24hPcnt > SmartPumpThreshold {
			distFromHigh := utils.DistanceFromHigh(currentCandle.Close, ticker24h.HighPrice24h)
			if distFromHigh < SmartPumpNearHighDist {
				return false
			}
		}
	}

	// Filter 5: MA Extension (> 8% for data mining, vs 4% for trading)
	sma200Deviation := features["sma200_deviation"].(float64)
	if sma200Deviation > MAExtensionThreshold {
		return false
	}

	// Filter 6: OI Long Exit
	isLongExit := features["is_long_exit"].(bool)
	if isLongExit {
		return false
	}

	// Filter 7: RS Weak (>= 0.0 for data mining, vs >= -1.2 for trading)
	if rs >= RSWeakThreshold {
		return false
	}

	// Filter 8: Funding (< -0.03 for data mining, vs < -0.015 for trading)
	if fundingRate < FundingAntiSqueeze {
		return false
	}

	// Filter 9: Volume (< 1.2x for data mining, vs < 1.8x for trading)
	if volumeRatio < VolumeMinRatio {
		return false
	}

	// Filter 10: Close Position (> 30%)
	if closePosition > ClosePositionMax {
		return false
	}

	// Filter 11: Support Age (dynamic)
	supportAgeMinutes := features["support_age_minutes"].(float64)
	change1h := features["change_1h"].(float64)

	minSupportAge := DefaultSupportAge
	if change1h < HighVolatilityThreshold1h {
		minSupportAge = HighVolatilitySupportAge
	}

	if supportAgeMinutes < minSupportAge {
		return false
	}

	// ALL FILTERS PASSED - this is an elite signal!
	return true
}

// calculateScore calculates trading score from features (for elite signals)
// Note: support and buffer parameters removed as they're redundant with features
func (e *DataMiningEngine) calculateScore(features map[string]interface{}) int {
	score := 0

	rs := features["rs"].(float64)
	volumeZScore := features["volume_z_score"].(float64)
	fundingRate := features["funding_rate"].(float64)
	closePosition := features["close_position"].(float64)
	supportAgeMinutes := features["support_age_minutes"].(float64)
	touchCount := features["support_touch_count"].(int)
	isConsolidation := features["is_consolidation_break"].(bool)
	isBTCDivergence := features["btc_divergence"].(bool)
	isAggressiveShort := features["is_aggressive_short"].(bool)
	isSmartPumpRollover := features["is_smart_pump_rollover"].(bool)

	// Weakness (up to 40 points)
	if rs < RSWeakThreshold {
		score += 30
		if rs < RSVeryWeakThreshold {
			score += 10
		}
	}

	// Volume Z-Score (up to 30 points)
	if volumeZScore > 2.0 {
		score += 10
		if volumeZScore > 2.5 {
			score += 10
			if volumeZScore > VolumeZScoreThreshold {
				score += 10
			}
		}
	}

	// Support age (20 points)
	if supportAgeMinutes > SupportAgeBonus {
		score += 20
	}

	// Funding (20 points)
	if fundingRate > FundingPositive {
		score += 20
	}

	// Close position (10 points)
	if closePosition < ClosePositionStrong {
		score += 10
	}

	// BTC divergence (10 points)
	if isBTCDivergence {
		score += 10
	}

	// OI aggressive short (15 points)
	if isAggressiveShort {
		score += 15
	}

	// Smart Pump Rollover (20 points)
	if isSmartPumpRollover {
		score += SmartPumpRolloverBonus
	}

	// Support touch count (up to 15 points)
	if touchCount >= 3 {
		score += 15
	} else if touchCount >= 2 {
		score += 10
	}

	// Consolidation (10 points)
	if isConsolidation {
		score += 10
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	return score
}

// emitSignal sends elite signal to Telegram (with cooldown)
func (e *DataMiningEngine) emitSignal(signal *models.Signal) {
	// Check cooldown (15 minutes like in strategy)
	if lastTime, exists := e.lastSignal[signal.Symbol]; exists {
		if time.Since(lastTime).Minutes() < 15 {
			return
		}
	}

	select {
	case e.signalChan <- signal:
		e.lastSignal[signal.Symbol] = time.Now()
		log.Info().
			Str("symbol", signal.Symbol).
			Int("score", signal.ScoreTotal).
			Msg("✅ ELITE SIGNAL - sent to Telegram")
	default:
		log.Warn().Str("symbol", signal.Symbol).Msg("signal channel full")
	}
}

// ===================================
// HELPER METHODS (copied from strategy)
// ===================================

func (e *DataMiningEngine) calculateRS(symbol string) (float64, error) {
	buffer := e.candleCache[symbol]
	if buffer == nil {
		return 0, fmt.Errorf("no data for symbol")
	}

	assetCount := buffer.Len()
	btcCount := e.btcBuffer.Len()

	if assetCount < MinRSLookback || btcCount < MinRSLookback {
		return 0, fmt.Errorf("insufficient data")
	}

	lookback := RSLookbackMinutes
	if assetCount < lookback {
		lookback = assetCount
	}
	if btcCount < lookback {
		lookback = btcCount
	}

	assetOld := buffer.Get(buffer.Len() - lookback).Close
	assetNew := buffer.Last().Close

	if assetOld == 0 {
		return 0, fmt.Errorf("asset old price is zero")
	}
	assetChange := ((assetNew - assetOld) / assetOld) * 100

	btcOld := e.btcBuffer.Get(e.btcBuffer.Len() - lookback).Close
	btcNew := e.btcBuffer.Last().Close

	if btcOld == 0 {
		return 0, fmt.Errorf("BTC old price is zero")
	}
	btcChange := ((btcNew - btcOld) / btcOld) * 100

	return assetChange - btcChange, nil
}

func (e *DataMiningEngine) calculateAvgVolume(buffer *strategy.RingBuffer) float64 {
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

func (e *DataMiningEngine) calculateVolumeZScore(buffer *strategy.RingBuffer, currentVolume float64) (zScore float64, meanVol float64, stdDevVol float64) {
	if buffer.Len() < 3 {
		return 0, 0, 0
	}

	window := VolumeZScoreWindow
	if buffer.Len() < window {
		window = buffer.Len()
	}

	historyLen := window - 1
	if historyLen < 2 {
		return 0, 0, 0
	}

	var sum float64
	startIdx := buffer.Len() - window
	if startIdx < 0 {
		startIdx = 0
	}
	for i := startIdx; i < buffer.Len()-1; i++ {
		sum += buffer.Get(i).Volume
	}
	meanVol = sum / float64(historyLen)

	var sumSquares float64
	for i := startIdx; i < buffer.Len()-1; i++ {
		diff := buffer.Get(i).Volume - meanVol
		sumSquares += diff * diff
	}
	stdDevVol = math.Sqrt(sumSquares / float64(historyLen-1))

	if stdDevVol == 0 {
		return 0, meanVol, 0
	}

	zScore = (currentVolume - meanVol) / stdDevVol
	return zScore, meanVol, stdDevVol
}

func (e *DataMiningEngine) calculateSMA200(buffer *strategy.RingBuffer) (sma float64, available bool) {
	if buffer.Len() < SMAMinCandles {
		return 0, false
	}

	window := SMAWindow
	if buffer.Len() < window {
		window = buffer.Len()
	}

	var sum float64
	startIdx := buffer.Len() - window
	for i := startIdx; i < buffer.Len(); i++ {
		sum += buffer.Get(i).Close
	}

	return sum / float64(window), true
}

func (e *DataMiningEngine) calculateChange1h(buffer *strategy.RingBuffer) float64 {
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

func (e *DataMiningEngine) calculateOIDivergence(symbol string, currentCandle models.Candle) (deltaOI float64, isAggressiveShort bool, isLongExit bool) {
	ticker := e.ticker24hStats[symbol]
	snapshot := e.oiSnapshots[symbol]

	if ticker == nil || snapshot == nil || ticker.OpenInterest <= 0 || snapshot.OpenInterest <= 0 {
		return 0, false, false
	}

	snapshotAge := time.Since(snapshot.Timestamp).Minutes()

	if snapshotAge < OISnapshotAgeMinutes {
		return 0, false, false
	}

	deltaOI = (ticker.OpenInterest - snapshot.OpenInterest) / snapshot.OpenInterest

	if _, stillTracked := e.ticker24hStats[symbol]; stillTracked {
		e.oiSnapshots[symbol] = &models.OISnapshot{
			OpenInterest: ticker.OpenInterest,
			Timestamp:    time.Now(),
		}
	}

	isPriceDown := currentCandle.Close < currentCandle.Open

	if isPriceDown {
		if deltaOI > OIDivergenceThreshold {
			isAggressiveShort = true
		} else if deltaOI < -OIDivergenceThreshold {
			isLongExit = true
		}
	}

	return deltaOI, isAggressiveShort, isLongExit
}

// UpdateFundingRates updates funding rates (called periodically)
func (e *DataMiningEngine) UpdateFundingRates(rates map[string]float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.fundingRates = make(map[string]float64, len(rates))
	for symbol, rate := range rates {
		e.fundingRates[symbol] = rate
	}
}

// UpdateTicker24hStats updates 24h statistics (called periodically)
func (e *DataMiningEngine) UpdateTicker24hStats(stats map[string]*models.Ticker24hStats) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	activeSymbols := make(map[string]struct{}, len(stats))

	e.ticker24hStats = make(map[string]*models.Ticker24hStats, len(stats))
	for symbol, stat := range stats {
		e.ticker24hStats[symbol] = stat
		activeSymbols[symbol] = struct{}{}

		if stat.OpenInterest > 0 {
			if _, exists := e.oiSnapshots[symbol]; !exists {
				e.oiSnapshots[symbol] = &models.OISnapshot{
					OpenInterest: stat.OpenInterest,
					Timestamp:    now,
				}
			}
		}
	}

	// Cleanup old OI snapshots
	for symbol := range e.oiSnapshots {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.oiSnapshots, symbol)
		}
	}
}

// GetSignalChannel returns channel for elite signals
func (e *DataMiningEngine) GetSignalChannel() <-chan *models.Signal {
	return e.signalChan
}

// CleanupInactiveSymbols removes data for inactive symbols
func (e *DataMiningEngine) CleanupInactiveSymbols(activeSymbols map[string]struct{}) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	cleaned := 0

	for symbol := range e.candleCache {
		if symbol == "BTCUSDT" {
			continue
		}
		if _, active := activeSymbols[symbol]; !active {
			delete(e.candleCache, symbol)
			cleaned++
		}
	}

	for symbol := range e.lastSignal {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.lastSignal, symbol)
		}
	}

	for symbol := range e.oiSnapshots {
		if _, active := activeSymbols[symbol]; !active {
			delete(e.oiSnapshots, symbol)
		}
	}

	return cleaned
}
