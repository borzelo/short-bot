package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/db"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/islamtagirov/millionaire-bot/internal/strategy"
	"github.com/islamtagirov/millionaire-bot/internal/training"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// Warm-up period: skip first N candles to ensure valid indicators
	// Need 200 for MA200, 120 for Z-Score, 60 for support detection
	WarmupPeriod = 200

	// Batch size for database inserts
	BatchSize = 1000

	// Worker pool size for parallel processing
	DefaultWorkers = 10
)

// CSVRow represents one row from processed CSV file
type CSVRow struct {
	Timestamp    int64
	Open         float64
	High         float64
	Low          float64
	Close        float64
	Volume       float64
	FundingRate  *float64 // nullable
	OpenInterest *float64 // nullable
	High24h      float64
	Low24h       float64
	Price24hPcnt float64
	VolumeUSD    float64
	Price15mMax  *float64
	Price15mMin  *float64
	Price15mClose *float64
	Price60mMax  *float64
	Price60mMin  *float64
	Price60mClose *float64
}

// ProcessResult tracks processing stats for one symbol
type ProcessResult struct {
	Symbol         string
	TotalRows      int
	BreakdownsFound int
	EliteSignals   int
	ShadowSignals  int
	Error          error
	Duration       time.Duration
}

func main() {
	// Parse flags
	dataDir := flag.String("dir", ".", "Directory containing CSV.GZ files")
	workers := flag.Int("workers", DefaultWorkers, "Number of parallel workers")
	databaseURL := flag.String("db", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Setup logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	log.Info().
		Str("data_dir", *dataDir).
		Int("workers", *workers).
		Msg("starting historical data import")

	ctx := context.Background()

	// Connect to database
	store, err := db.New(ctx, *databaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer store.Close()

	// Load BTC data first (needed for RS calculation)
	log.Info().Msg("loading BTC reference data...")
	btcFile := filepath.Join(*dataDir, "BTCUSDT_processed.csv.gz")
	btcData, err := loadCSVFile(btcFile)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load BTC data")
	}
	log.Info().Int("btc_rows", len(btcData)).Msg("BTC data loaded")

	// Build BTC map for fast lookup
	btcMap := make(map[int64]*CSVRow, len(btcData))
	for i := range btcData {
		btcMap[btcData[i].Timestamp] = &btcData[i]
	}

	// Note: We don't build a global btcBuffer here because it would contain
	// unsynchronized data (last 200 candles out of 262k).
	// Instead, we'll use btcMap directly for timestamp-synchronized lookups.

	// Find all symbol files
	files, err := filepath.Glob(filepath.Join(*dataDir, "*_processed.csv.gz"))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to list files")
	}

	// Filter out BTC (already processed)
	symbolFiles := []string{}
	for _, file := range files {
		if !strings.Contains(file, "BTCUSDT") {
			symbolFiles = append(symbolFiles, file)
		}
	}

	log.Info().Int("total_symbols", len(symbolFiles)).Msg("found symbol files")

	// Setup worker pool
	jobs := make(chan string, len(symbolFiles))
	results := make(chan ProcessResult, len(symbolFiles))
	var wg sync.WaitGroup

	// Start workers
	for w := 1; w <= *workers; w++ {
		wg.Add(1)
		go worker(w, jobs, results, btcMap, store, &wg)
	}

	// Queue jobs
	startTime := time.Now()
	for _, file := range symbolFiles {
		jobs <- file
	}
	close(jobs)

	// Collect results in background
	go func() {
		wg.Wait()
		close(results)
	}()

	// Print progress
	totalBreakdowns := 0
	totalElite := 0
	totalShadow := 0
	processedSymbols := 0

	for result := range results {
		processedSymbols++
		if result.Error != nil {
			log.Error().
				Err(result.Error).
				Str("symbol", result.Symbol).
				Msg("failed to process symbol")
			continue
		}

		totalBreakdowns += result.BreakdownsFound
		totalElite += result.EliteSignals
		totalShadow += result.ShadowSignals

		log.Info().
			Str("symbol", result.Symbol).
			Int("rows", result.TotalRows).
			Int("breakdowns", result.BreakdownsFound).
			Int("elite", result.EliteSignals).
			Int("shadow", result.ShadowSignals).
			Dur("duration", result.Duration).
			Int("progress", processedSymbols).
			Int("total", len(symbolFiles)).
			Msg("symbol processed")
	}

	totalDuration := time.Since(startTime)

	log.Info().
		Int("symbols", processedSymbols).
		Int("total_breakdowns", totalBreakdowns).
		Int("elite_signals", totalElite).
		Int("shadow_signals", totalShadow).
		Dur("total_duration", totalDuration).
		Msg("✅ import completed")
}

func worker(
	id int,
	jobs <-chan string,
	results chan<- ProcessResult,
	btcMap map[int64]*CSVRow,
	store *db.Store,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for file := range jobs {
		result := processSymbolFile(file, btcMap, store)
		results <- result
	}
}

func processSymbolFile(
	file string,
	btcMap map[int64]*CSVRow,
	store *db.Store,
) ProcessResult {
	startTime := time.Now()
	symbol := extractSymbol(file)

	result := ProcessResult{
		Symbol: symbol,
	}

	// Load CSV
	rows, err := loadCSVFile(file)
	if err != nil {
		result.Error = fmt.Errorf("load CSV: %w", err)
		return result
	}

	result.TotalRows = len(rows)

	if len(rows) < WarmupPeriod {
		result.Error = fmt.Errorf("insufficient data: %d rows < %d warmup", len(rows), WarmupPeriod)
		return result
	}

	// Build RingBuffer for warm-up period
	buffer := strategy.NewRingBuffer(200)
	for i := 0; i < WarmupPeriod; i++ {
		candle := rowToCandle(&rows[i], symbol)
		buffer.Push(candle)
	}

	// Process remaining rows
	batch := []*models.TrainingData{}
	ctx := context.Background()

	for i := WarmupPeriod; i < len(rows); i++ {
		row := &rows[i]
		candle := rowToCandle(row, symbol)
		buffer.Push(candle)

		// Get corresponding BTC row
		btcRow := btcMap[row.Timestamp]
		if btcRow == nil {
			// Gap in BTC data (shouldn't happen due to synchronization)
			log.Debug().
				Str("symbol", symbol).
				Int64("timestamp", row.Timestamp).
				Msg("missing BTC data for timestamp")
			continue
		}

		// Analyze breakdown
		trainingData := analyzeHistoricalBreakdown(symbol, candle, row, buffer, btcRow, btcMap)
		if trainingData != nil {
			result.BreakdownsFound++

			if trainingData.IsShadowMode {
				result.ShadowSignals++
			} else {
				result.EliteSignals++
			}

			batch = append(batch, trainingData)

			// Batch insert when size reached
			if len(batch) >= BatchSize {
				if err := batchInsert(ctx, store, batch); err != nil {
					result.Error = fmt.Errorf("batch insert: %w", err)
					return result
				}
				batch = []*models.TrainingData{}
			}
		}
	}

	// Flush remaining batch
	if len(batch) > 0 {
		if err := batchInsert(ctx, store, batch); err != nil {
			result.Error = fmt.Errorf("final batch insert: %w", err)
			return result
		}
	}

	result.Duration = time.Since(startTime)
	return result
}

func analyzeHistoricalBreakdown(
	symbol string,
	candle models.Candle,
	csvRow *CSVRow,
	buffer *strategy.RingBuffer,
	btcRow *CSVRow,
	btcMap map[int64]*CSVRow,
) *models.TrainingData {
	// PHASE 1: STRUCTURE DETECTION (breakdown-first!)

	// Detect support level
	allCandles := buffer.LastN(buffer.Len())
	support := strategy.DetectSupport(allCandles)
	if support == nil {
		return nil
	}

	// Check if price broke below support
	if candle.Close >= support.Price {
		return nil
	}

	// Confirm no immediate bounceback
	if !strategy.ConfirmNoBounceback(allCandles, support.Price) {
		return nil
	}

	// PHASE 2: CALCULATE FEATURES

	features := calculateFeaturesFromCSV(symbol, candle, csvRow, buffer, btcRow, btcMap, support)

	// PHASE 3: APPLY QUALITY FILTERS

	isShadowMode := !passesQualityFilters(features, csvRow)

	// PHASE 4: BUILD TRAINING DATA WITH OUTCOMES FROM CSV

	return &models.TrainingData{
		Symbol:            symbol,
		EntryPrice:        candle.Close,
		EntrySupportLevel: support.Price,
		Features:          features,
		Price15mMax:       csvRow.Price15mMax,
		Price15mMin:       csvRow.Price15mMin,
		Price15mClose:     csvRow.Price15mClose,
		Price60mMax:       csvRow.Price60mMax,
		Price60mMin:       csvRow.Price60mMin,
		Price60mClose:     csvRow.Price60mClose,
		IsShadowMode:      isShadowMode,
	}
}

func calculateFeaturesFromCSV(
	symbol string,
	candle models.Candle,
	csvRow *CSVRow,
	buffer *strategy.RingBuffer,
	btcRow *CSVRow,
	btcMap map[int64]*CSVRow,
	support *models.SupportLevel,
) map[string]interface{} {
	features := make(map[string]interface{})

	// 1. Relative Strength (RS)
	rs := calculateRS(candle, buffer, btcMap)
	features["rs"] = rs

	// 2. Volume metrics
	avgVolume := calculateAvgVolume(buffer)
	volumeRatio := 0.0
	if avgVolume > 0 {
		volumeRatio = candle.Volume / avgVolume
	}
	zScore, meanVol, stdDevVol := calculateVolumeZScore(buffer, candle.Volume)

	features["volume_ratio"] = volumeRatio
	features["volume_z_score"] = zScore
	features["volume_mean"] = meanVol
	features["volume_stddev"] = stdDevVol
	features["avg_volume"] = avgVolume

	// 3. Funding rate (NaN → 0.0 as per user requirement)
	fundingRate := 0.0
	if csvRow.FundingRate != nil {
		fundingRate = *csvRow.FundingRate
	}
	features["funding_rate"] = fundingRate

	// 4. 24h statistics (from CSV - already calculated)
	features["high_price_24h"] = csvRow.High24h
	features["low_price_24h"] = csvRow.Low24h
	features["price_24h_pcnt"] = csvRow.Price24hPcnt

	ndr := 0.0
	if csvRow.Low24h > 0 {
		ndr = (csvRow.High24h - csvRow.Low24h) / csvRow.Low24h
	}
	features["ndr_24h"] = ndr

	distFromHigh := 0.0
	if csvRow.High24h > 0 {
		distFromHigh = (csvRow.High24h - candle.Close) / csvRow.High24h
	}
	features["dist_from_high_pct"] = distFromHigh

	// 5. Open Interest (nullable - skip OI filters if NaN)
	oiDeltaPct := 0.0
	isAggressiveShort := false
	isLongExit := false
	// Note: OI delta calculation requires previous OI which we don't track in CSV processing
	// For historical data, we skip OI divergence detection
	features["oi_delta_pct"] = oiDeltaPct
	features["is_aggressive_short"] = isAggressiveShort
	features["is_long_exit"] = isLongExit

	// 6. Close position within candle
	candleRange := candle.High - candle.Low
	closePosition := 0.0
	if candleRange > 0 {
		closePosition = (candle.Close - candle.Low) / candleRange
	}
	features["close_position"] = closePosition

	// 7. Support level features
	supportAge := candle.Timestamp.Sub(support.Timestamp).Minutes()
	features["support_age_minutes"] = supportAge
	features["support_touch_count"] = support.TouchCount
	features["is_consolidation_break"] = support.IsConsolidation

	// 8. MA200 deviation
	ma200 := calculateMA(buffer, 200)
	ma200Deviation := 0.0
	if ma200 > 0 {
		ma200Deviation = (candle.Close - ma200) / ma200
	}
	features["sma200_deviation"] = ma200Deviation

	// 9. 1-hour price change
	change1h := 0.0
	if buffer.Len() >= 60 {
		oldPrice := buffer.Get(buffer.Len() - 60).Close
		if oldPrice > 0 {
			change1h = (candle.Close - oldPrice) / oldPrice
		}
	}
	features["change_1h"] = change1h

	// 10. BTC divergence (asset 1h change vs BTC 1h change)
	btcChange1h := 0.0
	if buffer.Len() >= 60 {
		// Get BTC candles at synchronized timestamps
		timestamp1hAgo := candle.Timestamp.Add(-60 * time.Minute).Unix() * 1000
		btcOldRow := btcMap[timestamp1hAgo]
		btcNewRow := btcMap[candle.Timestamp.Unix() * 1000]

		if btcOldRow != nil && btcNewRow != nil && btcOldRow.Close > 0 {
			btcChange1h = (btcNewRow.Close - btcOldRow.Close) / btcOldRow.Close
		}
	}
	btcDivergence := change1h - btcChange1h
	features["btc_divergence"] = btcDivergence

	// 11. Smart pump rollover detection
	isSmartPumpRollover := false
	if csvRow.Price24hPcnt > training.SmartPumpThreshold &&
		distFromHigh > training.SmartPumpRolloverDist &&
		zScore > training.VolumeZScoreThreshold {
		isSmartPumpRollover = true
	}
	features["is_smart_pump_rollover"] = isSmartPumpRollover

	// 12. Is current candle red?
	isRedCandle := candle.Close < candle.Open
	features["is_red_candle"] = isRedCandle

	// 13. Time features (UTC)
	features["hour_of_day"] = candle.Timestamp.UTC().Hour()
	features["day_of_week"] = int(candle.Timestamp.UTC().Weekday())

	// 14. Candle OHLCV
	features["candle_high"] = candle.High
	features["candle_low"] = candle.Low
	features["candle_open"] = candle.Open
	features["candle_close"] = candle.Close
	features["candle_volume"] = candle.Volume
	features["candle_range"] = candleRange

	return features
}

func passesQualityFilters(features map[string]interface{}, csvRow *CSVRow) bool {
	// Extract key features
	rs := features["rs"].(float64)
	volumeRatio := features["volume_ratio"].(float64)
	fundingRate := features["funding_rate"].(float64)
	closePosition := features["close_position"].(float64)
	ndr := features["ndr_24h"].(float64)
	price24hPcnt := csvRow.Price24hPcnt
	distFromHigh := features["dist_from_high_pct"].(float64)
	ma200Deviation := features["sma200_deviation"].(float64)
	isLongExit := features["is_long_exit"].(bool)

	// FILTER 1: Volatility (NDR < 3%)
	if ndr < training.VolatilityNDRMin {
		return false
	}

	// FILTER 2: Oversold 24h (< -40%)
	if price24hPcnt < training.OversoldThreshold24h {
		return false
	}

	// FILTER 3: Max pump (> 30%)
	if price24hPcnt > training.MaxPumpThreshold {
		return false
	}

	// FILTER 4a: Smart pump near high (too close to 24h high - reversal risk)
	if price24hPcnt > training.SmartPumpThreshold && distFromHigh < training.SmartPumpNearHighDist {
		return false
	}

	// FILTER 4b: Smart pump rollover (pumped but rolling over with volume)
	isSmartPumpRollover := features["is_smart_pump_rollover"].(bool)
	if isSmartPumpRollover {
		return false
	}

	// FILTER 5: MA extension (> 8%)
	if ma200Deviation > training.MAExtensionThreshold {
		return false
	}

	// FILTER 6: OI Long Exit (squeeze risk)
	if isLongExit {
		return false
	}

	// FILTER 7: RS threshold (>= 0.0% - weaker assets only)
	if rs >= training.RSWeakThreshold {
		return false
	}

	// FILTER 8: Funding anti-squeeze (< -0.03%)
	if fundingRate < training.FundingAntiSqueeze {
		return false
	}

	// FILTER 9: Volume ratio (>= 1.2x)
	if volumeRatio < training.VolumeMinRatio {
		return false
	}

	// FILTER 10: Close position (< 30% - breakout with conviction)
	if closePosition > training.ClosePositionMaxPct {
		return false
	}

	// FILTER 11: Support age (dynamic based on touch count)
	supportAge := features["support_age_minutes"].(float64)
	touchCount := features["support_touch_count"].(int)

	minSupportAge := float64(training.SupportMinAge)
	if touchCount >= 3 {
		minSupportAge = float64(training.SupportMinAgeMultiTouch)
	}

	if supportAge < minSupportAge {
		return false
	}

	// ALL FILTERS PASSED - this is an ELITE signal
	return true
}

// ============================================
// HELPER FUNCTIONS
// ============================================

func loadCSVFile(filename string) ([]CSVRow, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gzReader.Close()

	csvReader := csv.NewReader(gzReader)

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Validate header (expect 18 columns)
	if len(header) != 18 {
		return nil, fmt.Errorf("invalid header: expected 18 columns, got %d", len(header))
	}

	// Read all rows
	rows := []CSVRow{}
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}

		row, err := parseCSVRow(record)
		if err != nil {
			return nil, fmt.Errorf("parse row: %w", err)
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func parseCSVRow(record []string) (CSVRow, error) {
	row := CSVRow{}

	// timestamp (milliseconds)
	timestamp, err := strconv.ParseInt(record[0], 10, 64)
	if err != nil {
		return row, fmt.Errorf("parse timestamp: %w", err)
	}
	row.Timestamp = timestamp

	// OHLCV
	row.Open, _ = strconv.ParseFloat(record[1], 64)
	row.High, _ = strconv.ParseFloat(record[2], 64)
	row.Low, _ = strconv.ParseFloat(record[3], 64)
	row.Close, _ = strconv.ParseFloat(record[4], 64)
	row.Volume, _ = strconv.ParseFloat(record[5], 64)

	// funding_rate (nullable)
	if record[6] != "" && record[6] != "NaN" {
		val, _ := strconv.ParseFloat(record[6], 64)
		row.FundingRate = &val
	}

	// open_interest (nullable)
	if record[7] != "" && record[7] != "NaN" {
		val, _ := strconv.ParseFloat(record[7], 64)
		row.OpenInterest = &val
	}

	// 24h stats
	row.High24h, _ = strconv.ParseFloat(record[8], 64)
	row.Low24h, _ = strconv.ParseFloat(record[9], 64)
	row.Price24hPcnt, _ = strconv.ParseFloat(record[10], 64)
	row.VolumeUSD, _ = strconv.ParseFloat(record[11], 64)

	// 15m outcomes (nullable)
	if record[12] != "" && record[12] != "NaN" {
		val, _ := strconv.ParseFloat(record[12], 64)
		row.Price15mMax = &val
	}
	if record[13] != "" && record[13] != "NaN" {
		val, _ := strconv.ParseFloat(record[13], 64)
		row.Price15mMin = &val
	}
	if record[14] != "" && record[14] != "NaN" {
		val, _ := strconv.ParseFloat(record[14], 64)
		row.Price15mClose = &val
	}

	// 60m outcomes (nullable)
	if record[15] != "" && record[15] != "NaN" {
		val, _ := strconv.ParseFloat(record[15], 64)
		row.Price60mMax = &val
	}
	if record[16] != "" && record[16] != "NaN" {
		val, _ := strconv.ParseFloat(record[16], 64)
		row.Price60mMin = &val
	}
	if record[17] != "" && record[17] != "NaN" {
		val, _ := strconv.ParseFloat(record[17], 64)
		row.Price60mClose = &val
	}

	return row, nil
}

func rowToCandle(row *CSVRow, symbol string) models.Candle {
	return models.Candle{
		Symbol:    symbol,
		Timestamp: time.Unix(row.Timestamp/1000, (row.Timestamp%1000)*1000000),
		Open:      row.Open,
		High:      row.High,
		Low:       row.Low,
		Close:     row.Close,
		Volume:    row.Volume,
	}
}

func extractSymbol(filepath string) string {
	base := filepath[strings.LastIndex(filepath, "/")+1:]
	return strings.TrimSuffix(base, "_processed.csv.gz")
}

func calculateRS(
	candle models.Candle,
	buffer *strategy.RingBuffer,
	btcMap map[int64]*CSVRow,
) float64 {
	lookback := training.RSLookbackMinutes // Use constant from training package

	if buffer.Len() < lookback {
		return 0.0
	}

	// Asset price change
	assetOld := buffer.Get(buffer.Len() - lookback).Close
	assetNew := candle.Close
	if assetOld == 0 {
		return 0.0
	}
	assetChange := ((assetNew - assetOld) / assetOld) * 100

	// BTC price change (synchronized by timestamp)
	timestampOld := candle.Timestamp.Add(-time.Duration(lookback) * time.Minute).Unix() * 1000
	timestampNew := candle.Timestamp.Unix() * 1000

	btcOldRow := btcMap[timestampOld]
	btcNewRow := btcMap[timestampNew]

	if btcOldRow == nil || btcNewRow == nil || btcOldRow.Close == 0 {
		return 0.0
	}

	btcChange := ((btcNewRow.Close - btcOldRow.Close) / btcOldRow.Close) * 100

	return assetChange - btcChange
}

func calculateAvgVolume(buffer *strategy.RingBuffer) float64 {
	window := training.VolumeAvgWindow // Use constant from training package
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

func calculateVolumeZScore(buffer *strategy.RingBuffer, currentVolume float64) (zScore, mean, stddev float64) {
	window := training.VolumeZScoreWindow // Use constant from training package
	if buffer.Len() < 3 {
		return 0, 0, 0
	}

	count := window
	if buffer.Len() < window {
		count = buffer.Len()
	}

	recentCandles := buffer.LastN(count)

	// Calculate mean
	var sum float64
	for _, c := range recentCandles {
		sum += c.Volume
	}
	mean = sum / float64(len(recentCandles))

	// Calculate standard deviation
	var variance float64
	for _, c := range recentCandles {
		diff := c.Volume - mean
		variance += diff * diff
	}
	variance /= float64(len(recentCandles))
	stddev = math.Sqrt(variance)

	// Calculate Z-Score
	if stddev > 0 {
		zScore = (currentVolume - mean) / stddev
	}

	return zScore, mean, stddev
}

func calculateMA(buffer *strategy.RingBuffer, period int) float64 {
	if buffer.Len() < period {
		return 0
	}

	candles := buffer.LastN(period)
	var sum float64
	for _, c := range candles {
		sum += c.Close
	}

	return sum / float64(len(candles))
}

func batchInsert(ctx context.Context, store *db.Store, batch []*models.TrainingData) error {
	for _, data := range batch {
		if err := store.SaveTrainingData(ctx, data); err != nil {
			return fmt.Errorf("save training data: %w", err)
		}
	}
	return nil
}
