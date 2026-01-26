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

func main() {
	setupLogger()

	log.Info().Msg("🚀 Starting Millionaire Bot")

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

	// Fetch mid-cap USDT futures
	assets, err := apiClient.GetTop100USDTFutures()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to fetch mid-cap USDT futures")
	}

	log.Info().Int("count", len(assets)).Msg("loaded mid-cap USDT futures")

	// Save assets to database
	for _, asset := range assets {
		if err := store.SaveAsset(ctx, &asset); err != nil {
			log.Error().Err(err).Str("symbol", asset.Symbol).Msg("failed to save asset")
		}
	}

	// Extract symbol list for WebSocket subscription
	symbols := extractSymbols(assets)

	// Ensure BTCUSDT is included for RS calculation
	symbols = ensureBTCIncluded(symbols)

	// Initialize strategy engine
	engine := strategy.NewEngine()

	// Funding rates updater
	updateFundingRates := func() {
		rates, _ := apiClient.GetFundingRates(symbols)
		engine.UpdateFundingRates(rates)
	}

	// Fetch funding rates at startup
	updateFundingRates()

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

	// Start periodic tasks
	go runPeriodicTasks(ctx, engine, updateFundingRates)

	log.Info().Msg("✅ Bot is running and monitoring markets")

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
	log.Warn().Msg("BTCUSDT not in top 100, adding it for RS calculation")
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

func runPeriodicTasks(ctx context.Context, engine *strategy.Engine, updateFundingRates func()) {
	statsTicker := time.NewTicker(5 * time.Minute)
	fundingTicker := time.NewTicker(5 * time.Minute)
	defer statsTicker.Stop()
	defer fundingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			stats := engine.GetStats()
			log.Info().Interface("stats", stats).Msg("engine statistics")
		case <-fundingTicker.C:
			updateFundingRates()
		}
	}
}
