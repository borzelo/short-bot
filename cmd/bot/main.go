package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/bybit"
	"github.com/islamtagirov/millionaire-bot/internal/config"
	"github.com/islamtagirov/millionaire-bot/internal/db"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/islamtagirov/millionaire-bot/internal/strategy"
	"github.com/islamtagirov/millionaire-bot/internal/telegram"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// v1.4.0: Minimum turnover for instrument selection
	MinTurnoverUSD = 5_000_000 // $5M/24h minimum
	// v1.4.0: Number of weak assets to monitor
	TopWeakCount = 50
)

func main() {
	setupLogger()

	log.Info().Msg("🚀 Starting Millionaire Bot v1.5.1")

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	log.Info().Msg("configuration loaded successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to database
	store, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer store.Close()

	// Initialize ByBit API client
	apiClient := bybit.NewAPIClient(cfg.ByBitAPIURL, cfg.ByBitAPIAltURL)

	// v1.4.0: Get ALL USDT Perp symbols with minimum liquidity
	log.Info().Float64("min_turnover", MinTurnoverUSD).Msg("fetching all instruments with minimum turnover")
	allSymbols, err := apiClient.GetAllInstruments(MinTurnoverUSD)
	if err != nil {
		log.Warn().Err(err).Msg("failed to fetch all instruments, using fallback")
		// Fallback to old method
		assets, _ := apiClient.GetTop100USDTFutures()
		allSymbols = extractSymbols(assets)
	}
	log.Info().Int("count", len(allSymbols)).Msg("loaded instruments for weakness scanning")

	// v1.4.0: Initialize WeaknessScanner
	weaknessScanner := strategy.NewWeaknessScanner(apiClient)

	// v1.4.0: Scan all symbols and calculate weakness scores
	log.Info().Msg("performing initial weakness scan (this may take a few minutes)...")
	if err := weaknessScanner.ScanAll(allSymbols); err != nil {
		log.Warn().Err(err).Msg("weakness scan failed, using all symbols")
	}

	// v1.4.0: Get top weak symbols for monitoring
	weakScores := weaknessScanner.GetTopWeak(TopWeakCount)
	symbols := strategy.GetSymbolsFromScores(weakScores)

	// Log selected weak symbols
	log.Info().
		Int("selected", len(symbols)).
		Int("total_scanned", len(allSymbols)).
		Msg("selected weakest assets for monitoring")

	// Ensure BTCUSDT is included for RS calculation
	symbols = ensureBTCIncluded(symbols)

	// Save selected assets to database
	for _, symbol := range symbols {
		asset := models.Asset{Symbol: symbol, IsTradable: true}
		if err := store.SaveAsset(ctx, &asset); err != nil {
			log.Debug().Err(err).Str("symbol", symbol).Msg("failed to save asset")
		}
	}

	// Initialize strategy engine
	engine := strategy.NewEngine()

	// Combined ticker data updater (v1.3.0: single API call for funding + 24h stats)
	updateTickerData := func() {
		data, err := apiClient.GetTickerData(symbols)
		if err != nil {
			log.Warn().Err(err).Msg("failed to fetch ticker data")
			return
		}
		if data == nil {
			log.Warn().Msg("ticker data is nil")
			return
		}
		engine.UpdateFundingRates(data.FundingRates)
		engine.UpdateTicker24hStats(data.Ticker24hStats)
	}

	// Fetch ticker data at startup
	updateTickerData()

	// Initialize Telegram notifier
	notifier := telegram.NewNotifier(cfg.TelegramBotToken, cfg.TelegramChatID)

	// Initialize and connect WebSocket client (handles reconnection internally)
	wsClient := bybit.NewWSClient(cfg.ByBitWSURL, symbols)
	if err := wsClient.Connect(ctx); err != nil {
		log.Fatal().Err(err).Msg("failed to connect to WebSocket")
	}
	defer wsClient.Close()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start processing goroutines
	go processCandles(ctx, engine, wsClient)
	go processSignals(ctx, engine, store, notifier)

	// v1.4.0: Weakness scanner update function (hourly)
	updateWeaknessScores := func() {
		log.Info().Msg("updating weakness scores...")
		if err := weaknessScanner.ScanAll(allSymbols); err != nil {
			log.Warn().Err(err).Msg("weakness scan update failed")
		}
		// Note: Dynamic WebSocket subscription update not implemented yet
		// This requires changes to WSClient to support UpdateSubscriptions()
	}

	// Start periodic tasks
	go runPeriodicTasks(ctx, engine, updateTickerData, updateWeaknessScores)

	log.Info().Msg("✅ Bot is running and monitoring weak assets")

	// Wait for shutdown signal
	<-sigChan
	log.Info().Msg("🛑 Shutdown signal received, stopping gracefully...")

	cancel()
	time.Sleep(2 * time.Second)

	log.Info().Msg("👋 Bot stopped")
}

func setupLogger() {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = zerolog.New(output).With().Timestamp().Caller().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

func extractSymbols(assets []models.Asset) []string {
	symbols := make([]string, len(assets))
	for i, asset := range assets {
		symbols[i] = asset.Symbol
	}
	return symbols
}

func ensureBTCIncluded(symbols []string) []string {
	for _, sym := range symbols {
		if sym == "BTCUSDT" {
			return symbols
		}
	}
	log.Warn().Msg("BTCUSDT not in selected symbols, adding it for RS calculation")
	return append(symbols, "BTCUSDT")
}

func processCandles(ctx context.Context, engine *strategy.Engine, wsClient *bybit.WSClient) {
	candleChan := wsClient.GetCandleChannel()

	for {
		select {
		case <-ctx.Done():
			return
		case candle := <-candleChan:
			engine.ProcessCandle(candle)
		}
	}
}

func processSignals(ctx context.Context, engine *strategy.Engine, store *db.Store, notifier *telegram.Notifier) {
	signalChan := engine.GetSignalChannel()

	for {
		select {
		case <-ctx.Done():
			return
		case signal := <-signalChan:
			// Save to database
			if err := store.SaveSignal(ctx, signal); err != nil {
				log.Error().Err(err).Str("symbol", signal.Symbol).Msg("failed to save signal")
				continue
			}

			// Send Telegram notification
			if err := notifier.SendSignal(signal); err != nil {
				log.Error().Err(err).Str("symbol", signal.Symbol).Msg("failed to send telegram notification")
			}
		}
	}
}

func runPeriodicTasks(ctx context.Context, engine *strategy.Engine, updateTickerData func(), updateWeaknessScores func()) {
	statsTicker := time.NewTicker(5 * time.Minute)
	tickerDataTicker := time.NewTicker(5 * time.Minute)
	weaknessTicker := time.NewTicker(15 * time.Minute) // v1.5.1: 15-min weakness scan (was 1h - too slow for crypto)
	defer statsTicker.Stop()
	defer tickerDataTicker.Stop()
	defer weaknessTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			stats := engine.GetStats()
			log.Info().Interface("stats", stats).Msg("engine statistics")
		case <-tickerDataTicker.C:
			updateTickerData()
		case <-weaknessTicker.C:
			updateWeaknessScores()
		}
	}
}
