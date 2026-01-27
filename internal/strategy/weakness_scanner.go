package strategy

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/bybit"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

// Rate limiting and worker pool constants for API calls
const (
	// v1.7.0: Parallel scanning with worker pool
	scanWorkers      = 5                      // Number of parallel workers (5 = ~50 req/sec with 100ms delay)
	apiRequestDelay  = 100 * time.Millisecond // 100ms between API calls per worker
	scanTimeout      = 5 * time.Minute        // Maximum time for entire scan
)

// WeaknessScanner scans all assets and ranks them by weakness (v1.4.0)
// v1.7.0: Uses worker pool for parallel API calls
type WeaknessScanner struct {
	client     *bybit.APIClient
	scorer     *WeaknessScorer
	scores     map[string]*models.WeaknessScore
	btcDaily   []models.HistoricalCandle // Cache of BTC daily candles
	btc4h      []models.HistoricalCandle // Cache of BTC 4h candles
	mu         sync.RWMutex
}

// NewWeaknessScanner creates a new scanner
func NewWeaknessScanner(client *bybit.APIClient) *WeaknessScanner {
	return &WeaknessScanner{
		client: client,
		scorer: NewWeaknessScorer(),
		scores: make(map[string]*models.WeaknessScore),
	}
}

// scanResult holds the result of scanning a single symbol
type scanResult struct {
	symbol string
	score  *models.WeaknessScore
	err    error
}

// ScanAll recalculates WeaknessScore for all symbols using parallel workers
// v1.7.0: Uses worker pool for ~5x faster scanning
// Should be called every 15 minutes
func (ws *WeaknessScanner) ScanAll(symbols []string) error {
	startTime := time.Now()
	log.Info().Int("symbols", len(symbols)).Int("workers", scanWorkers).Msg("starting parallel weakness scan")

	// Fetch BTC candles first (used as benchmark)
	btcDaily, err := ws.client.GetKlines("BTCUSDT", "D", 200)
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch BTC daily candles")
		// Continue without BTC comparison
	}

	btc4h, err := ws.client.GetKlines("BTCUSDT", "240", 50)
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch BTC 4h candles")
	}

	ws.mu.Lock()
	ws.btcDaily = btcDaily
	ws.btc4h = btc4h
	ws.mu.Unlock()

	// Get ticker data for funding rates (single API call)
	tickerData, err := ws.client.GetTickerData(symbols)
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch ticker data for weakness scoring")
	}

	// Filter symbols (exclude BTCUSDT)
	filteredSymbols := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if symbol != "BTCUSDT" {
			filteredSymbols = append(filteredSymbols, symbol)
		}
	}

	// Create channels for worker pool
	symbolChan := make(chan string, len(filteredSymbols))
	resultChan := make(chan scanResult, len(filteredSymbols))

	// Counters for stats
	var processed, errors int64

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < scanWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ws.scanWorker(workerID, symbolChan, resultChan, tickerData)
		}(i)
	}

	// Send symbols to workers
	go func() {
		for _, symbol := range filteredSymbols {
			symbolChan <- symbol
		}
		close(symbolChan)
	}()

	// Wait for all workers to finish, then close results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results with timeout
	newScores := make(map[string]*models.WeaknessScore)
	timeout := time.After(scanTimeout)

collectLoop:
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				break collectLoop // Channel closed, all done
			}
			if result.err != nil {
				atomic.AddInt64(&errors, 1)
				log.Debug().Err(result.err).Str("symbol", result.symbol).Msg("failed to calculate weakness score")
			} else {
				atomic.AddInt64(&processed, 1)
				newScores[result.symbol] = result.score
			}
		case <-timeout:
			log.Warn().
				Int64("processed", atomic.LoadInt64(&processed)).
				Int64("errors", atomic.LoadInt64(&errors)).
				Msg("weakness scan timed out")
			break collectLoop
		}
	}

	// Update scores atomically
	ws.mu.Lock()
	ws.scores = newScores
	ws.mu.Unlock()

	elapsed := time.Since(startTime)
	log.Info().
		Int64("processed", atomic.LoadInt64(&processed)).
		Int64("errors", atomic.LoadInt64(&errors)).
		Dur("elapsed", elapsed).
		Float64("symbols_per_sec", float64(processed)/elapsed.Seconds()).
		Msg("parallel weakness scan completed")

	return nil
}

// scanWorker processes symbols from the channel with rate limiting
func (ws *WeaknessScanner) scanWorker(workerID int, symbols <-chan string, results chan<- scanResult, tickerData *bybit.TickerData) {
	for symbol := range symbols {
		score, err := ws.calculateWeaknessScore(symbol, tickerData)
		if err != nil {
			results <- scanResult{symbol: symbol, err: err}
		} else {
			score.Symbol = symbol
			results <- scanResult{symbol: symbol, score: score}
		}

		// Rate limiting per worker to respect ByBit API limits
		time.Sleep(apiRequestDelay)
	}
}

func (ws *WeaknessScanner) calculateWeaknessScore(symbol string, tickerData *bybit.TickerData) (*models.WeaknessScore, error) {
	// Fetch daily candles for this symbol
	dailyCandles, err := ws.client.GetKlines(symbol, "D", 200)
	if err != nil {
		return nil, err
	}

	if len(dailyCandles) < 7 {
		return nil, fmt.Errorf("insufficient daily candles: need 7, got %d", len(dailyCandles))
	}

	// Fetch 4h candles for RS24h
	candles4h, _ := ws.client.GetKlines(symbol, "240", 50)

	// Calculate metrics
	ws.mu.RLock()
	btcDaily := ws.btcDaily
	btc4h := ws.btc4h
	ws.mu.RUnlock()

	// RS7d (daily candles)
	rs7d := CalculateRS7d(dailyCandles, btcDaily)

	// RS24h (4h candles)
	rs24h := CalculateRS24h(candles4h, btc4h)

	// Current price
	currentPrice := dailyCandles[len(dailyCandles)-1].Close

	// MA50 and MA200
	ma50 := CalculateMA(dailyCandles, 50)
	ma200 := CalculateMA(dailyCandles, 200)
	priceVsMA50 := CalculatePriceVsMA(currentPrice, ma50)
	priceVsMA200 := CalculatePriceVsMA(currentPrice, ma200)

	// Volume decline
	volumeDecline := CalculateVolumeDecline(dailyCandles)

	// Funding rate from ticker data
	fundingRate := 0.0
	if tickerData != nil {
		fundingRate = tickerData.FundingRates[symbol]
	}

	// Calculate score
	return ws.scorer.CalculateScore(
		rs7d,
		rs24h,
		priceVsMA50,
		priceVsMA200,
		volumeDecline,
		fundingRate,
	), nil
}

// GetTopWeak returns top N weakest assets sorted by TotalScore descending
func (ws *WeaknessScanner) GetTopWeak(n int) []models.WeaknessScore {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	// Convert map to slice
	scores := make([]models.WeaknessScore, 0, len(ws.scores))
	for _, score := range ws.scores {
		scores = append(scores, *score)
	}

	// Sort by TotalScore descending (higher = weaker)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].TotalScore > scores[j].TotalScore
	})

	// Return top N
	if len(scores) > n {
		scores = scores[:n]
	}

	log.Info().
		Int("requested", n).
		Int("returned", len(scores)).
		Msg("returning top weak assets")

	return scores
}

// GetScore returns the weakness score for a specific symbol
func (ws *WeaknessScanner) GetScore(symbol string) *models.WeaknessScore {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.scores[symbol]
}

// GetSymbols extracts symbol names from weakness scores
func GetSymbolsFromScores(scores []models.WeaknessScore) []string {
	symbols := make([]string, len(scores))
	for i, score := range scores {
		symbols[i] = score.Symbol
	}
	return symbols
}
