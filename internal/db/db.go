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
