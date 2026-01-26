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

	// Get tickers with 24h volume
	tickerURL := fmt.Sprintf("%s/v5/market/tickers?category=linear", c.baseURL)
	resp, err := c.client.Get(tickerURL)
	if err != nil {
		return nil, fmt.Errorf("fetch tickers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
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

	log.Info().Int("count", len(top100Symbols)).Msg("filtered top USDT futures")

	// Get instrument info for top 100
	instrumentURL := fmt.Sprintf("%s/v5/market/instruments-info?category=linear", c.baseURL)
	resp, err = c.client.Get(instrumentURL)
	if err != nil {
		return nil, fmt.Errorf("fetch instruments: %w", err)
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read instruments: %w", err)
	}

	var instrumentResp instrumentResponse
	if err := json.Unmarshal(body, &instrumentResp); err != nil {
		return nil, fmt.Errorf("unmarshal instruments: %w", err)
	}

	if instrumentResp.RetCode != 0 {
		return nil, fmt.Errorf("bybit api error: %s", instrumentResp.RetMsg)
	}

	// Create map for fast lookup
	symbolSet := make(map[string]bool)
	for _, sym := range top100Symbols {
		symbolSet[sym] = true
	}

	volumeMap := make(map[string]float64)
	for _, v := range volumes {
		volumeMap[v.symbol] = v.volume
	}

	// Build assets list
	var assets []models.Asset
	for _, inst := range instrumentResp.Result.List {
		if !symbolSet[inst.Symbol] {
			continue
		}

		if inst.Status != "Trading" {
			continue
		}

		var tickSize, minLotSize float64
		fmt.Sscanf(inst.PriceFilter.TickSize, "%f", &tickSize)
		fmt.Sscanf(inst.LotSizeFilter.MinOrderQty, "%f", &minLotSize)

		assets = append(assets, models.Asset{
			Symbol:      inst.Symbol,
			TickSize:    tickSize,
			MinLotSize:  minLotSize,
			IsTradable:  true,
			Volume24h:   volumeMap[inst.Symbol],
		})
	}

	log.Info().Int("count", len(assets)).Msg("loaded tradable instruments")
	return assets, nil
}
