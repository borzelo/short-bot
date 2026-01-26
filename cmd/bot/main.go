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
	"github.com/islamtagirov/millionaire-bot/internal/strategy"
	"github.com/islamtagirov/millionaire-bot/internal/telegram"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Setup logger
	setupLogger()

	log.Info().Msg("🚀 Starting Millionaire Bot")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	log.Info().Msg("configuration loaded successfully")

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to database
	store, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer store.Close()

	// Initialize ByBit API client
	apiClient := bybit.NewAPIClient(cfg.ByBitAPIURL)

	// Fetch top 100 USDT futures
	assets, err := apiClient.GetTop100USDTFutures()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to fetch top 100 USDT futures")
	}

	log.Info().Int("count", len(assets)).Msg("loaded top USDT futures")

	// Save assets to database
	for _, asset := range assets {
		if err := store.SaveAsset(ctx, &asset); err != nil {
			log.Error().Err(err).Str("symbol", asset.Symbol).Msg("failed to save asset")
		}
	}

	// Extract symbol list for WebSocket subscription
	symbols := make([]string, len(assets))
	for i, asset := range assets {
		symbols[i] = asset.Symbol
	}

	// Ensure BTCUSDT is included for RS calculation
	hasBTC := false
	for _, sym := range symbols {
		if sym == "BTCUSDT" {
			hasBTC = true
			break
		}
	}
	if !hasBTC {
		log.Warn().Msg("BTCUSDT not in top 100, adding it for RS calculation")
		symbols = append(symbols, "BTCUSDT")
	}

	// Initialize strategy engine
	engine := strategy.NewEngine()

	// Initialize Telegram notifier
	notifier := telegram.NewNotifier(cfg.TelegramBotToken, cfg.TelegramChatID)

	// Initialize WebSocket client
	wsClient := bybit.NewWSClient(cfg.ByBitWSURL, symbols)

	// Connect to WebSocket
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
	go handleReconnect(ctx, wsClient, symbols, cfg.ByBitWSURL)

	// Log stats periodically
	statsTicker := time.NewTicker(5 * time.Minute)
	defer statsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				stats := engine.GetStats()
				log.Info().
					Interface("stats", stats).
					Msg("engine statistics")
			}
		}
	}()

	log.Info().Msg("✅ Bot is running and monitoring markets")

	// Wait for shutdown signal
	<-sigChan
	log.Info().Msg("🛑 Shutdown signal received, stopping gracefully...")

	// Cancel context to stop all goroutines
	cancel()

	// Give goroutines time to finish
	time.Sleep(2 * time.Second)

	log.Info().Msg("👋 Bot stopped")
}

func setupLogger() {
	// Pretty console logging for development
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log.Logger = zerolog.New(output).With().Timestamp().Caller().Logger()

	// Set log level
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
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

func handleReconnect(ctx context.Context, wsClient *bybit.WSClient, symbols []string, wsURL string) {
	reconnectChan := wsClient.GetReconnectChannel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnectChan:
			log.Warn().Msg("reconnection triggered, attempting to reconnect...")

			// Close old connection
			wsClient.Close()

			// Wait before reconnecting
			time.Sleep(5 * time.Second)

			// Create new WebSocket client
			newWSClient := bybit.NewWSClient(wsURL, symbols)

			// Try to reconnect with exponential backoff
			maxRetries := 5
			for i := 0; i < maxRetries; i++ {
				if err := newWSClient.Connect(ctx); err != nil {
					log.Error().Err(err).Int("attempt", i+1).Msg("reconnection failed")
					time.Sleep(time.Duration(1<<uint(i)) * time.Second) // Exponential backoff
					continue
				}

				log.Info().Msg("✅ reconnected successfully")
				return
			}

			log.Fatal().Msg("failed to reconnect after max retries")
		}
	}
}
