package strategy

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/bybit"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

// Rate limiting constants for API calls
const (
	apiRequestDelay = 100 * time.Millisecond // 100ms between API calls to avoid rate limiting
)

// WeaknessScanner scans all assets and ranks them by weakness (v1.4.0)
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

// ScanAll recalculates WeaknessScore for all symbols
// Should be called every hour
func (ws *WeaknessScanner) ScanAll(symbols []string) error {
	log.Info().Int("symbols", len(symbols)).Msg("starting weakness scan")

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

	// Get ticker data for funding rates
	tickerData, _ := ws.client.GetTickerData(symbols)

	// Process each symbol
	newScores := make(map[string]*models.WeaknessScore)
	processed := 0
	errors := 0

	for _, symbol := range symbols {
		if symbol == "BTCUSDT" {
			continue // Skip BTC itself
		}

		score, err := ws.calculateWeaknessScore(symbol, tickerData)
		if err != nil {
			log.Debug().Err(err).Str("symbol", symbol).Msg("failed to calculate weakness score")
			errors++
			continue
		}

		score.Symbol = symbol
		newScores[symbol] = score
		processed++

		// Rate limiting to avoid API throttling
		time.Sleep(apiRequestDelay)
	}

	ws.mu.Lock()
	ws.scores = newScores
	ws.mu.Unlock()

	log.Info().
		Int("processed", processed).
		Int("errors", errors).
		Msg("weakness scan completed")

	return nil
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
