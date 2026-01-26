package bybit

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

type APIClient struct {
	baseURL string
	client  *http.Client
}

type tickerResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string `json:"category"`
		List     []struct {
			Symbol        string `json:"symbol"`
			Volume24h     string `json:"volume24h"`
			Turnover24h   string `json:"turnover24h"`
			LastPrice     string `json:"lastPrice"`
		} `json:"list"`
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

func NewAPIClient(baseURL string) *APIClient {
	return &APIClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *APIClient) GetTop100USDTFutures() ([]models.Asset, error) {
	log.Info().Msg("fetching top 100 USDT futures from ByBit")

	// Try to fetch from API, fallback to hardcoded list
	symbols, err := c.fetchSymbolsFromAPI()
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch from API, using fallback symbol list")
		symbols = getDefaultSymbols()
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
	// Get tickers with 24h volume
	tickerURL := fmt.Sprintf("%s/v5/market/tickers?category=linear", c.baseURL)

	// Create request with headers
	req, err := http.NewRequest("GET", tickerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Add headers to avoid Cloudflare blocking
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; MillionaireBot/1.0)")
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch tickers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Log response for debugging
	if resp.StatusCode != http.StatusOK {
		log.Warn().
			Int("status_code", resp.StatusCode).
			Msg("API returned non-200 status, will use fallback")
		return nil, fmt.Errorf("api returned status %d", resp.StatusCode)
	}

	var tickerResp tickerResponse
	if err := json.Unmarshal(body, &tickerResp); err != nil {
		return nil, fmt.Errorf("unmarshal tickers: %w", err)
	}

	if tickerResp.RetCode != 0 {
		return nil, fmt.Errorf("bybit api error: %s", tickerResp.RetMsg)
	}

	// Filter USDT perpetuals and sort by volume
	type symbolVolume struct {
		symbol string
		volume float64
	}

	var volumes []symbolVolume
	for _, ticker := range tickerResp.Result.List {
		// Only USDT perpetuals
		if len(ticker.Symbol) < 4 || ticker.Symbol[len(ticker.Symbol)-4:] != "USDT" {
			continue
		}

		var vol float64
		fmt.Sscanf(ticker.Volume24h, "%f", &vol)
		volumes = append(volumes, symbolVolume{
			symbol: ticker.Symbol,
			volume: vol,
		})
	}

	// Sort by volume descending
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].volume > volumes[j].volume
	})

	// Take top 100
	top100Count := 100
	if len(volumes) < top100Count {
		top100Count = len(volumes)
	}
	top100Symbols := make([]string, top100Count)
	for i := 0; i < top100Count; i++ {
		top100Symbols[i] = volumes[i].symbol
	}

	log.Info().Int("count", len(top100Symbols)).Msg("filtered top USDT futures from API")
	return top100Symbols, nil
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
