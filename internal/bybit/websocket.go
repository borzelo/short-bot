package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

// WSClient handles WebSocket connection to ByBit with auto-reconnect
type WSClient struct {
	url        string
	symbols    []string
	conn       *websocket.Conn
	mu         sync.RWMutex
	candleChan chan models.Candle
	ctx        context.Context
	cancel     context.CancelFunc
	connected  atomic.Bool
	wg         sync.WaitGroup
}

type wsSubscription struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type wsKlineResponse struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Interval  string `json:"interval"`
		Open      string `json:"open"`
		Close     string `json:"close"`
		High      string `json:"high"`
		Low       string `json:"low"`
		Volume    string `json:"volume"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
		Timestamp int64  `json:"timestamp"`
	} `json:"data"`
}

const (
	maxReconnectAttempts = 10
	baseReconnectDelay   = 1 * time.Second
	maxReconnectDelay    = 60 * time.Second
	pingInterval         = 20 * time.Second
	readDeadline         = 60 * time.Second
)

func NewWSClient(url string, symbols []string) *WSClient {
	return &WSClient{
		url:        url,
		symbols:    symbols,
		candleChan: make(chan models.Candle, 1000),
	}
}

// Connect establishes WebSocket connection and starts auto-reconnect loop
func (c *WSClient) Connect(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	if err := c.connect(); err != nil {
		return err
	}

	// Start routines with WaitGroup for graceful shutdown
	c.wg.Add(2)
	go c.pingRoutine()
	go c.readRoutine()

	return nil
}

// connect establishes a single WebSocket connection
func (c *WSClient) connect() error {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
		ReadBufferSize:   8192,
		WriteBufferSize:  8192,
	}

	log.Info().Str("url", c.url).Msg("connecting to ByBit WebSocket")

	conn, resp, err := dialer.Dial(c.url, nil)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial websocket: %w (status: %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial websocket: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	// Configure connection
	conn.SetReadDeadline(time.Now().Add(readDeadline))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})

	c.connected.Store(true)
	log.Info().Msg("WebSocket connected successfully")

	// Subscribe to all symbols
	if err := c.subscribe(); err != nil {
		c.closeConnection()
		return fmt.Errorf("subscribe: %w", err)
	}

	return nil
}

// reconnect attempts to reconnect with exponential backoff
func (c *WSClient) reconnect() bool {
	c.connected.Store(false)
	c.closeConnection()

	delay := baseReconnectDelay
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		select {
		case <-c.ctx.Done():
			return false
		case <-time.After(delay):
		}

		log.Info().
			Int("attempt", attempt).
			Int("max_attempts", maxReconnectAttempts).
			Msg("attempting to reconnect")

		if err := c.connect(); err != nil {
			log.Error().Err(err).Int("attempt", attempt).Msg("reconnection failed")

			// Exponential backoff with cap
			delay = delay * 2
			if delay > maxReconnectDelay {
				delay = maxReconnectDelay
			}
			continue
		}

		log.Info().Int("attempt", attempt).Msg("✅ reconnected successfully")
		return true
	}

	log.Error().Msg("failed to reconnect after max attempts")
	return false
}

func (c *WSClient) subscribe() error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("connection not established")
	}

	// ByBit allows max 10 subscriptions per request
	const batchSize = 10
	for i := 0; i < len(c.symbols); i += batchSize {
		end := i + batchSize
		if end > len(c.symbols) {
			end = len(c.symbols)
		}

		batch := c.symbols[i:end]
		topics := make([]string, len(batch))
		for j, symbol := range batch {
			topics[j] = fmt.Sprintf("kline.1.%s", symbol)
		}

		sub := wsSubscription{
			Op:   "subscribe",
			Args: topics,
		}

		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("write subscription: %w", err)
		}

		log.Info().
			Int("batch_num", i/batchSize+1).
			Int("symbols_count", len(batch)).
			Msg("subscribed to kline batch")

		// Small delay between batches to avoid rate limiting
		time.Sleep(100 * time.Millisecond)
	}

	log.Info().Int("total_symbols", len(c.symbols)).Msg("all subscriptions completed")
	return nil
}

func (c *WSClient) pingRoutine() {
	defer c.wg.Done()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if !c.connected.Load() {
				continue
			}

			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				continue
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Warn().Err(err).Msg("ping failed")
				// Don't trigger reconnect here, readRoutine will handle it
			}
		}
	}
}

func (c *WSClient) readRoutine() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if !c.connected.Load() {
			// Try to reconnect
			if !c.reconnect() {
				log.Fatal().Msg("unable to maintain WebSocket connection, shutting down")
				return
			}
			continue
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			c.connected.Store(false)
			continue
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Warn().Err(err).Msg("read message error, will reconnect")
			c.connected.Store(false)
			continue
		}

		c.processMessage(message)
	}
}

func (c *WSClient) processMessage(message []byte) {
	var klineResp wsKlineResponse
	if err := json.Unmarshal(message, &klineResp); err != nil {
		// Skip non-kline messages (pings, subscription confirmations, etc.)
		return
	}

	if klineResp.Topic == "" || len(klineResp.Data) == 0 {
		return
	}

	for _, k := range klineResp.Data {
		if !k.Confirm {
			continue
		}

		candle, err := c.parseCandle(klineResp.Topic, k)
		if err != nil {
			log.Warn().Err(err).Str("topic", klineResp.Topic).Msg("failed to parse candle")
			continue
		}

		// Validate candle before sending
		if err := validateCandle(candle); err != nil {
			log.Warn().Err(err).Str("symbol", candle.Symbol).Msg("invalid candle data")
			continue
		}

		select {
		case c.candleChan <- candle:
		case <-c.ctx.Done():
			return
		default:
			log.Warn().Str("symbol", candle.Symbol).Msg("candle channel full, dropping candle")
		}
	}
}

// parseCandle safely parses kline data with proper error handling
func (c *WSClient) parseCandle(topic string, k struct {
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Interval  string `json:"interval"`
	Open      string `json:"open"`
	Close     string `json:"close"`
	High      string `json:"high"`
	Low       string `json:"low"`
	Volume    string `json:"volume"`
	Turnover  string `json:"turnover"`
	Confirm   bool   `json:"confirm"`
	Timestamp int64  `json:"timestamp"`
}) (models.Candle, error) {
	// Extract symbol from topic "kline.1.BTCUSDT"
	var symbol string
	if _, err := fmt.Sscanf(topic, "kline.1.%s", &symbol); err != nil || symbol == "" {
		return models.Candle{}, fmt.Errorf("invalid topic format: %s", topic)
	}

	open, err := strconv.ParseFloat(k.Open, 64)
	if err != nil {
		return models.Candle{}, fmt.Errorf("parse open: %w", err)
	}

	high, err := strconv.ParseFloat(k.High, 64)
	if err != nil {
		return models.Candle{}, fmt.Errorf("parse high: %w", err)
	}

	low, err := strconv.ParseFloat(k.Low, 64)
	if err != nil {
		return models.Candle{}, fmt.Errorf("parse low: %w", err)
	}

	closePrice, err := strconv.ParseFloat(k.Close, 64)
	if err != nil {
		return models.Candle{}, fmt.Errorf("parse close: %w", err)
	}

	volume, err := strconv.ParseFloat(k.Volume, 64)
	if err != nil {
		return models.Candle{}, fmt.Errorf("parse volume: %w", err)
	}

	return models.Candle{
		Symbol:    symbol,
		Timestamp: time.UnixMilli(k.Start),
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePrice,
		Volume:    volume,
	}, nil
}

// validateCandle checks if candle data is logically valid
func validateCandle(c models.Candle) error {
	if c.Symbol == "" {
		return fmt.Errorf("empty symbol")
	}
	if c.Timestamp.IsZero() {
		return fmt.Errorf("zero timestamp")
	}
	if c.High < c.Low {
		return fmt.Errorf("high (%.8f) < low (%.8f)", c.High, c.Low)
	}
	if c.Close > c.High || c.Close < c.Low {
		return fmt.Errorf("close (%.8f) outside [low: %.8f, high: %.8f]", c.Close, c.Low, c.High)
	}
	if c.Open > c.High || c.Open < c.Low {
		return fmt.Errorf("open (%.8f) outside [low: %.8f, high: %.8f]", c.Open, c.Low, c.High)
	}
	if c.Volume < 0 {
		return fmt.Errorf("negative volume: %.8f", c.Volume)
	}
	// Check for zero prices (likely data error)
	if c.Open == 0 || c.High == 0 || c.Low == 0 || c.Close == 0 {
		return fmt.Errorf("zero price detected")
	}
	return nil
}

func (c *WSClient) closeConnection() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		c.conn.WriteMessage(websocket.CloseMessage, closeMsg)
		c.conn.Close()
		c.conn = nil
	}
}

func (c *WSClient) GetCandleChannel() <-chan models.Candle {
	return c.candleChan
}

// Close gracefully shuts down the WebSocket client
func (c *WSClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}

	// Wait for routines to finish with timeout
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Warn().Msg("timeout waiting for WebSocket routines to finish")
	}

	c.closeConnection()
}
