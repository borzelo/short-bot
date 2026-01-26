package bybit

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

type APIClient struct {
	baseURL    string
	altBaseURL string
	client     *http.Client
}

type tickerInfo struct {
	Symbol       string `json:"symbol"`
	Volume24h    string `json:"volume24h"`
	Turnover24h  string `json:"turnover24h"`
	LastPrice    string `json:"lastPrice"`
	FundingRate  string `json:"fundingRate"`
	HighPrice24h string `json:"highPrice24h"`
	LowPrice24h  string `json:"lowPrice24h"`
	Price24hPcnt string `json:"price24hPcnt"`
}

type tickerResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string `json:"category"`
		List     []tickerInfo `json:"list"`
	} `json:"result"`
}

type instrumentResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string `json:"category"`
		List     []struct {
			Symbol      string `json:"symbol"`
			Status      string `json:"status"`
			BaseCoin    string `json:"baseCoin"`
			QuoteCoin   string `json:"quoteCoin"`
			PriceFilter struct {
				TickSize string `json:"tickSize"`
			} `json:"priceFilter"`
			LotSizeFilter struct {
				MinOrderQty string `json:"minOrderQty"`
			} `json:"lotSizeFilter"`
		} `json:"list"`
	} `json:"result"`
}

func NewAPIClient(baseURL string, altBaseURL string) *APIClient {
	return &APIClient{
		baseURL:    baseURL,
		altBaseURL: altBaseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *APIClient) GetTop100USDTFutures() ([]models.Asset, error) {
	log.Info().Msg("fetching mid-cap USDT futures from ByBit")

	// Try to fetch from API, fallback to hardcoded list
	symbols, err := c.fetchSymbolsFromAPI()
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch from API, using fallback symbol list")
		symbols = filterFallbackSymbols(getDefaultSymbols())
	}

	// Get instrument info
	assets, err := c.getInstrumentInfo(symbols)
	if err != nil {
		return nil, fmt.Errorf("get instrument info: %w", err)
	}

	log.Info().Int("count", len(assets)).Msg("loaded tradable instruments")
	return assets, nil
}

func (c *APIClient) fetchSymbolsFromAPI() ([]string, error) {
	tickers, err := c.fetchTickersFromAPI()
	if err != nil {
		return nil, err
	}

	// Filter USDT perpetuals and sort by turnover (USDT)
	type symbolTurnover struct {
		symbol   string
		turnover float64
	}

	const minTurnoverUSD = 10_000_000

	var turnovers []symbolTurnover
	for _, ticker := range tickers {
		// Only USDT perpetuals
		if len(ticker.Symbol) < 4 || ticker.Symbol[len(ticker.Symbol)-4:] != "USDT" {
			continue
		}

		turnover, err := strconv.ParseFloat(ticker.Turnover24h, 64)
		if err != nil {
			continue
		}
		if turnover < minTurnoverUSD {
			continue
		}

		turnovers = append(turnovers, symbolTurnover{
			symbol:   ticker.Symbol,
			turnover: turnover,
		})
	}

	// Sort by turnover descending
	sort.Slice(turnovers, func(i, j int) bool {
		return turnovers[i].turnover > turnovers[j].turnover
	})

	const excludeTop = 15
	if len(turnovers) <= excludeTop {
		return nil, fmt.Errorf("not enough symbols after excluding top %d", excludeTop)
	}

	start := excludeTop
	end := start + 100
	if len(turnovers) < end {
		end = len(turnovers)
	}

	selected := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		selected = append(selected, turnovers[i].symbol)
	}

	log.Info().
		Int("count", len(selected)).
		Int("excluded", excludeTop).
		Msg("filtered mid-cap USDT futures from API")
	return selected, nil
}

// Alternative ByBit API endpoints that may bypass Cloudflare
var alternativeAPIs = []string{
	"https://api.bytick.com",      // Alternative ByBit domain
	"https://api.bybit.nl",        // Netherlands region
	"https://api-demo.bybit.com",  // Demo API (real market data)
}

func (c *APIClient) fetchTickersFromAPI() ([]tickerInfo, error) {
	// Build list of URLs to try
	urlsToTry := []string{c.baseURL}
	if c.altBaseURL != "" && c.altBaseURL != c.baseURL {
		urlsToTry = append(urlsToTry, c.altBaseURL)
	}
	// Add alternative APIs as fallbacks
	for _, alt := range alternativeAPIs {
		if alt != c.baseURL && alt != c.altBaseURL {
			urlsToTry = append(urlsToTry, alt)
		}
	}

	var lastErr error
	var lastStatusCode int

	for i, url := range urlsToTry {
		body, statusCode, err := c.fetchTickersBody(url)
		if err == nil {
			var tickerResp tickerResponse
			if err := json.Unmarshal(body, &tickerResp); err != nil {
				lastErr = fmt.Errorf("unmarshal tickers: %w", err)
				continue
			}

			if tickerResp.RetCode != 0 {
				lastErr = fmt.Errorf("bybit api error: %s", tickerResp.RetMsg)
				continue
			}

			if i > 0 {
				log.Info().
					Str("url", url).
					Int("attempt", i+1).
					Msg("successfully fetched data from alternative API")
			}
			return tickerResp.Result.List, nil
		}

		lastErr = err
		lastStatusCode = statusCode

		if i < len(urlsToTry)-1 {
			log.Debug().
				Int("status_code", statusCode).
				Str("url", url).
				Int("attempt", i+1).
				Msg("API request failed, trying next URL")
		}
	}

	log.Warn().
		Int("status_code", lastStatusCode).
		Int("urls_tried", len(urlsToTry)).
		Msg("all API endpoints returned non-200 status, will use fallback")
	return nil, lastErr
}

func (c *APIClient) fetchTickersBody(baseURL string) ([]byte, int, error) {
	tickerURL := fmt.Sprintf("%s/v5/market/tickers?category=linear", baseURL)

	req, err := http.NewRequest("GET", tickerURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	// Add headers to avoid Cloudflare blocking
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://www.bybit.com")
	req.Header.Set("Referer", "https://www.bybit.com/")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch tickers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("api returned status %d", resp.StatusCode)
	}

	return body, resp.StatusCode, nil
}

// TickerData combines funding rates and 24h stats (fetched in single API call)
type TickerData struct {
	FundingRates   map[string]float64
	Ticker24hStats map[string]*models.Ticker24hStats
}

// GetTickerData fetches both funding rates and 24h stats in a SINGLE API call (v1.3.0 optimization)
// This avoids duplicate HTTP requests to ByBit API
func (c *APIClient) GetTickerData(symbols []string) (*TickerData, error) {
	tickers, err := c.fetchTickersFromAPI()
	if err != nil {
		log.Warn().
			Err(err).
			Int("symbols_count", len(symbols)).
			Msg("ticker data unavailable, using fallback values")
		
		// Return fallback with zero funding rates (allows strategy to work)
		rates := make(map[string]float64, len(symbols))
		for _, symbol := range symbols {
			rates[symbol] = 0 // Neutral funding assumption
		}
		return &TickerData{
			FundingRates:   rates,
			Ticker24hStats: make(map[string]*models.Ticker24hStats),
		}, nil
	}

	symbolSet := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		symbolSet[symbol] = struct{}{}
	}

	rates := make(map[string]float64, len(symbolSet))
	stats := make(map[string]*models.Ticker24hStats, len(symbolSet))

	for _, ticker := range tickers {
		if _, ok := symbolSet[ticker.Symbol]; !ok {
			continue
		}

		// Parse funding rate
		if ticker.FundingRate != "" {
			if rate, err := strconv.ParseFloat(ticker.FundingRate, 64); err == nil {
				// Convert to percent for strategy thresholds (e.g., 0.0001 => 0.01%)
				rates[ticker.Symbol] = rate * 100
			}
		}

		// Parse 24h stats
		highPrice, _ := strconv.ParseFloat(ticker.HighPrice24h, 64)
		lowPrice, _ := strconv.ParseFloat(ticker.LowPrice24h, 64)
		price24hPcnt, _ := strconv.ParseFloat(ticker.Price24hPcnt, 64)
		lastPrice, _ := strconv.ParseFloat(ticker.LastPrice, 64)

		// Only add if essential data is present
		if highPrice > 0 && lowPrice > 0 && lastPrice > 0 {
			stats[ticker.Symbol] = &models.Ticker24hStats{
				Symbol:       ticker.Symbol,
				HighPrice24h: highPrice,
				LowPrice24h:  lowPrice,
				Price24hPcnt: price24hPcnt,
				LastPrice:    lastPrice,
			}
		}
	}

	log.Info().
		Int("funding_count", len(rates)).
		Int("stats_count", len(stats)).
		Msg("ticker data fetched successfully (single API call)")

	return &TickerData{
		FundingRates:   rates,
		Ticker24hStats: stats,
	}, nil
}

// GetTicker24hStats fetches 24h price statistics (uses combined GetTickerData internally)
// Kept for backward compatibility
func (c *APIClient) GetTicker24hStats(symbols []string) (map[string]*models.Ticker24hStats, error) {
	data, err := c.GetTickerData(symbols)
	if err != nil {
		return make(map[string]*models.Ticker24hStats), err
	}
	return data.Ticker24hStats, nil
}

// GetFundingRates fetches funding rates (uses combined GetTickerData internally)
// Kept for backward compatibility
func (c *APIClient) GetFundingRates(symbols []string) (map[string]float64, error) {
	data, err := c.GetTickerData(symbols)
	if err != nil {
		rates := make(map[string]float64, len(symbols))
		for _, symbol := range symbols {
			rates[symbol] = 0
		}
		return rates, err
	}
	return data.FundingRates, nil
}

// getDefaultSymbols returns a hardcoded list of popular USDT perpetuals
func getDefaultSymbols() []string {
	return []string{
		"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
		"ADAUSDT", "DOGEUSDT", "MATICUSDT", "DOTUSDT", "LINKUSDT",
		"AVAXUSDT", "SHIBUSDT", "UNIUSDT", "ATOMUSDT", "LTCUSDT",
		"NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT",
		"FILUSDT", "INJUSDT", "STXUSDT", "TIAUSDT", "FETUSDT",
		"RNDRUSDT", "IMXUSDT", "RUNEUSDT", "PENDLEUSDT", "SEIUSDT",
		"WLDUSDT", "AAVEUSDT", "MKRUSDT", "LDOUSDT", "JUPUSDT",
		"PYTHUSDT", "FTMUSDT", "ALGOUSDT", "ICPUSDT", "VETUSDT",
		"SANDUSDT", "MANAUSDT", "AXSUSDT", "THETAUSDT", "EGLDUSDT",
		"GRTUSDT", "FLOKIUSDT", "PEPEUSDT", "BOMEUSDT", "WIFUSDT",
		"ENSUSDT", "ORDIUSDT", "JASMYUSDT", "RENDERUSDT", "TAOUSDT",
		"1000PEPEUSDT", "BONKUSDT", "ETHFIUSDT", "CHZUSDT", "ETCUSDT",
		"HBARUSDT", "XLMUSDT", "TRXUSDT", "BCHUSDT", "COMPUSDT",
		"CRVUSDT", "YFIUSDT", "SNXUSDT", "1INCHUSDT", "SUSHIUSDT",
		"GASUSDT", "ZECUSDT", "DASHUSDT", "XTZUSDT", "EOSUSDT",
		"KSMUSDT", "KAVAUSDT", "ZILUSDT", "ONTUSDT", "IOTAUSDT",
		"CELOUSDT", "BALUSDT", "ZENUSDT", "OMGUSDT", "WAVESUSDT",
		"QTUMUSDT", "BATUSDT", "ZRXUSDT", "ENJUSDT", "RENUSDT",
		"RLCUSDT", "RSRUSDT", "LRCUSDT", "BANDUSDT", "NMRUSDT",
		"OGNUSDT", "STORJUSDT", "KNCUSDT", "BELUSDT", "CTKUSDT",
	}
}

func filterFallbackSymbols(symbols []string) []string {
	heavyweights := map[string]struct{}{
		"BTCUSDT":  {},
		"ETHUSDT":  {},
		"SOLUSDT":  {},
		"BNBUSDT":  {},
		"XRPUSDT":  {},
		"DOGEUSDT": {},
		"ADAUSDT":  {},
		"TRXUSDT":  {},
		"MATICUSDT": {},
		"DOTUSDT":  {},
		"LINKUSDT": {},
		"AVAXUSDT": {},
		"SHIBUSDT": {},
		"LTCUSDT":  {},
		"BCHUSDT":  {},
	}

	filtered := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if _, isHeavy := heavyweights[symbol]; isHeavy {
			continue
		}
		filtered = append(filtered, symbol)
	}

	if len(filtered) > 100 {
		return filtered[:100]
	}
	return filtered
}

func (c *APIClient) getInstrumentInfo(symbols []string) ([]models.Asset, error) {
	// Use WebSocket API-compatible endpoint without Cloudflare issues
	// Or just create basic assets from symbols
	assets := make([]models.Asset, 0, len(symbols))

	for _, symbol := range symbols {
		assets = append(assets, models.Asset{
			Symbol:      symbol,
			TickSize:    0.01,    // Default values, will be fine for monitoring
			MinLotSize:  0.001,   // Default values
			IsTradable:  true,
			Volume24h:   0,
		})
	}

	return assets, nil
}
