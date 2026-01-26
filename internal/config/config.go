package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL      string
	ByBitWSURL       string
	ByBitAPIURL      string
	ByBitAPIKey      string
	ByBitAPISecret   string
	TelegramBotToken string
	TelegramChatID   string
	LogLevel         string
}

func Load() (*Config, error) {
	// Load .env file if it exists (for local development)
	_ = godotenv.Load()

	cfg := &Config{
		DatabaseURL:      getEnv("DATABASE_URL", ""),
		ByBitWSURL:       getEnv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear"),
		ByBitAPIURL:      getEnv("BYBIT_API_URL", "https://api.bybit.com"),
		ByBitAPIKey:      getEnv("BYBIT_API_KEY", ""),
		ByBitAPISecret:   getEnv("BYBIT_API_SECRET", ""),
		TelegramBotToken: getEnv("TELEGRAM_BOT_TOKEN", ""),
		TelegramChatID:   getEnv("TELEGRAM_CHAT_ID", ""),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.TelegramBotToken == "" {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN is required")
	}
	if c.TelegramChatID == "" {
		return fmt.Errorf("TELEGRAM_CHAT_ID is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
