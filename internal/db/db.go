package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Configure pool settings
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = time.Minute

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET timezone = 'UTC'")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	store := &Store{pool: pool}

	// Run migrations
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	log.Info().Msg("database connection established")
	return store, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS assets (
			symbol VARCHAR(20) PRIMARY KEY,
			tick_size DECIMAL NOT NULL,
			min_lot_size DECIMAL NOT NULL,
			is_tradable BOOLEAN DEFAULT TRUE
		)`,
		`CREATE TABLE IF NOT EXISTS signals (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			symbol VARCHAR(20) REFERENCES assets(symbol),
			price_trigger DECIMAL NOT NULL,
			level_broken DECIMAL NOT NULL,
			breakdown_volume DECIMAL,
			score_rs DECIMAL,
			score_total INT,
			meta JSONB
		)`,
		`CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals(symbol, created_at DESC)`,
		// v1.9.0: Training data table for ML model
		`CREATE TABLE IF NOT EXISTS training_data (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			symbol VARCHAR(20) NOT NULL,

			-- ENTRY POINT (T=0)
			entry_price DECIMAL NOT NULL,
			entry_support_level DECIMAL,

			-- FEATURES (INPUTS) - What the model learns from
			features JSONB NOT NULL,

			-- RESULTS AFTER 15 MINUTES (OUTPUTS)
			-- Filled by background worker after 15 min
			price_15m_max DECIMAL,
			price_15m_min DECIMAL,
			price_15m_close DECIMAL,

			-- RESULTS AFTER 60 MINUTES (OUTPUTS)
			-- Filled by background worker after 60 min
			price_60m_max DECIMAL,
			price_60m_min DECIMAL,
			price_60m_close DECIMAL,

			-- META INFORMATION
			is_shadow_mode BOOLEAN DEFAULT FALSE,
			ml_prediction FLOAT DEFAULT NULL
		)`,
		// Index for finding pending rows (where worker needs to fill 60m results)
		`CREATE INDEX IF NOT EXISTS idx_training_pending ON training_data(created_at)
		 WHERE price_60m_close IS NULL`,
		// Index for querying by symbol
		`CREATE INDEX IF NOT EXISTS idx_training_symbol ON training_data(symbol, created_at DESC)`,
	}

	for _, migration := range migrations {
		if _, err := s.pool.Exec(ctx, migration); err != nil {
			return fmt.Errorf("execute migration: %w", err)
		}
	}

	log.Info().Msg("database migrations completed")
	return nil
}

func (s *Store) SaveAsset(ctx context.Context, asset *models.Asset) error {
	query := `
		INSERT INTO assets (symbol, tick_size, min_lot_size, is_tradable)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (symbol) DO UPDATE SET
			tick_size = EXCLUDED.tick_size,
			min_lot_size = EXCLUDED.min_lot_size,
			is_tradable = EXCLUDED.is_tradable
	`

	_, err := s.pool.Exec(ctx, query,
		asset.Symbol,
		asset.TickSize,
		asset.MinLotSize,
		asset.IsTradable,
	)

	if err != nil {
		return fmt.Errorf("save asset: %w", err)
	}

	return nil
}

func (s *Store) SaveSignal(ctx context.Context, signal *models.Signal) error {
	metaJSON, err := json.Marshal(signal.Meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	query := `
		INSERT INTO signals (symbol, price_trigger, level_broken, breakdown_volume, score_rs, score_total, meta)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, created_at
	`

	err = s.pool.QueryRow(ctx, query,
		signal.Symbol,
		signal.PriceTrigger,
		signal.LevelBroken,
		signal.BreakdownVolume,
		signal.ScoreRS,
		signal.ScoreTotal,
		metaJSON,
	).Scan(&signal.ID, &signal.CreatedAt)

	if err != nil {
		return fmt.Errorf("save signal: %w", err)
	}

	log.Info().
		Str("symbol", signal.Symbol).
		Int("score", signal.ScoreTotal).
		Int("signal_id", signal.ID).
		Msg("signal saved to database")

	return nil
}

// SaveTrainingData saves a training data point for ML model (v1.9.0)
// Captures ALL breakdown events regardless of quality filters
func (s *Store) SaveTrainingData(ctx context.Context, data *models.TrainingData) error {
	featuresJSON, err := json.Marshal(data.Features)
	if err != nil {
		return fmt.Errorf("marshal features: %w", err)
	}

	query := `
		INSERT INTO training_data (
			symbol, entry_price, entry_support_level, features, is_shadow_mode
		)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at
	`

	err = s.pool.QueryRow(ctx, query,
		data.Symbol,
		data.EntryPrice,
		data.EntrySupportLevel,
		featuresJSON,
		data.IsShadowMode,
	).Scan(&data.ID, &data.CreatedAt)

	if err != nil {
		return fmt.Errorf("save training data: %w", err)
	}

	shadowLabel := "real"
	if data.IsShadowMode {
		shadowLabel = "shadow"
	}

	log.Debug().
		Str("symbol", data.Symbol).
		Str("mode", shadowLabel).
		Int("data_id", data.ID).
		Msg("training data saved to database")

	return nil
}

// UpdateTrainingOutcomes updates 15m/60m outcome fields for a training data row (v1.9.0)
// Used when processing historical data where outcomes are known
func (s *Store) UpdateTrainingOutcomes(ctx context.Context, id int, outcomes *models.TrainingOutcomes) error {
	query := `
		UPDATE training_data
		SET
			price_15m_max = $1,
			price_15m_min = $2,
			price_15m_close = $3,
			price_60m_max = $4,
			price_60m_min = $5,
			price_60m_close = $6
		WHERE id = $7
	`

	_, err := s.pool.Exec(ctx, query,
		outcomes.Price15mMax,
		outcomes.Price15mMin,
		outcomes.Price15mClose,
		outcomes.Price60mMax,
		outcomes.Price60mMin,
		outcomes.Price60mClose,
		id,
	)

	if err != nil {
		return fmt.Errorf("update training outcomes: %w", err)
	}

	return nil
}
