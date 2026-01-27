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
// v1.7.0: Added fatalChan for graceful shutdown instead of log.Fatal()
type WSClient struct {
	url        string
	symbols    []string // Initial symbols (kept for backwards compatibility)
	conn       *websocket.Conn
	mu         sync.RWMutex
	writeMu    sync.Mutex // Separate mutex for WebSocket writes (gorilla/websocket is not thread-safe for writes)
	candleChan chan models.Candle
	fatalChan  chan error // v1.7.0: Channel for fatal errors (triggers graceful shutdown)
	ctx        context.Context
	cancel     context.CancelFunc
	connected  atomic.Bool
	wg         sync.WaitGroup

	// Dynamic subscription tracking (v1.6.0)
	subscribedSymbols map[string]struct{}  // Set of currently subscribed symbols
	lastActive        map[string]time.Time // When symbol was last in top weak list
	subMu             sync.RWMutex         // Mutex for subscription maps
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
	subscriptionBatchSize = 10                  // ByBit allows max 10 subscriptions per request
	batchDelay            = 100 * time.Millisecond
	maxSubscriptionsWarn  = 200                 // Soft limit for warning
)

func NewWSClient(url string, symbols []string) *WSClient {
	// Initialize subscription tracking with initial symbols
	subscribedSymbols := make(map[string]struct{}, len(symbols))
	lastActive := make(map[string]time.Time, len(symbols))
	now := time.Now()

	for _, sym := range symbols {
		subscribedSymbols[sym] = struct{}{}
		lastActive[sym] = now
	}

	return &WSClient{
		url:               url,
		symbols:           symbols,
		candleChan:        make(chan models.Candle, 1000),
		fatalChan:         make(chan error, 1), // v1.7.0: Buffered to prevent blocking
		subscribedSymbols: subscribedSymbols,
		lastActive:        lastActive,
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

	// Get all currently tracked symbols (important for reconnect!)
	c.subMu.RLock()
	symbols := make([]string, 0, len(c.subscribedSymbols))
	for sym := range c.subscribedSymbols {
		symbols = append(symbols, sym)
	}
	c.subMu.RUnlock()

	if len(symbols) == 0 {
		log.Warn().Msg("no symbols to subscribe to")
		return nil
	}

	for i := 0; i < len(symbols); i += subscriptionBatchSize {
		end := i + subscriptionBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		topics := make([]string, len(batch))
		for j, symbol := range batch {
			topics[j] = fmt.Sprintf("kline.1.%s", symbol)
		}

		sub := wsSubscription{
			Op:   "subscribe",
			Args: topics,
		}

		// Use writeMu to prevent concurrent writes
		c.writeMu.Lock()
		err := conn.WriteJSON(sub)
		c.writeMu.Unlock()

		if err != nil {
			return fmt.Errorf("write subscription: %w", err)
		}

		log.Info().
			Int("batch_num", i/subscriptionBatchSize+1).
			Int("symbols_count", len(batch)).
			Msg("subscribed to kline batch")

		// Small delay between batches to avoid rate limiting
		time.Sleep(batchDelay)
	}

	log.Info().Int("total_symbols", len(symbols)).Msg("all subscriptions completed")
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

			// Use writeMu to prevent concurrent writes (gorilla/websocket is not thread-safe)
			c.writeMu.Lock()
			err := conn.WriteMessage(websocket.PingMessage, nil)
			c.writeMu.Unlock()

			if err != nil {
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
				// v1.7.0: Instead of log.Fatal, send error to fatalChan for graceful shutdown
				err := fmt.Errorf("unable to maintain WebSocket connection after %d attempts", maxReconnectAttempts)
				log.Error().Err(err).Msg("WebSocket reconnection failed, triggering graceful shutdown")
				
				// Non-blocking send to fatal channel
				select {
				case c.fatalChan <- err:
				default:
					// Channel already has an error, skip
				}
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
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn != nil {
		// Use writeMu for the close message write
		c.writeMu.Lock()
		closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		conn.WriteMessage(websocket.CloseMessage, closeMsg)
		c.writeMu.Unlock()

		conn.Close()
	}
}

func (c *WSClient) GetCandleChannel() <-chan models.Candle {
	return c.candleChan
}

// GetFatalChan returns channel that receives fatal errors requiring shutdown
// v1.7.0: Used for graceful shutdown instead of log.Fatal()
func (c *WSClient) GetFatalChan() <-chan error {
	return c.fatalChan
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

// AddSubscriptions subscribes to new symbols (ignores already subscribed)
// This method is thread-safe and can be called from any goroutine
func (c *WSClient) AddSubscriptions(symbols []string) error {
	if !c.connected.Load() {
		return fmt.Errorf("not connected")
	}

	now := time.Now()

	// Filter new symbols and update lastActive for all
	c.subMu.Lock()
	var newSymbols []string
	for _, sym := range symbols {
		// Update lastActive for all symbols (even already subscribed)
		c.lastActive[sym] = now

		// Check if already subscribed
		if _, exists := c.subscribedSymbols[sym]; !exists {
			newSymbols = append(newSymbols, sym)
			c.subscribedSymbols[sym] = struct{}{}
		}
	}

	totalSubscribed := len(c.subscribedSymbols)
	c.subMu.Unlock()

	// Warn if approaching soft limit
	if totalSubscribed > maxSubscriptionsWarn {
		log.Warn().
			Int("total_subscribed", totalSubscribed).
			Int("soft_limit", maxSubscriptionsWarn).
			Msg("subscription count exceeds soft limit")
	}

	// No new symbols to subscribe
	if len(newSymbols) == 0 {
		log.Debug().Msg("no new symbols to subscribe")
		return nil
	}

	log.Info().
		Int("new_symbols", len(newSymbols)).
		Int("total_subscribed", totalSubscribed).
		Msg("adding new WebSocket subscriptions")

	// Subscribe to new symbols in batches
	if err := c.subscribeToSymbols(newSymbols); err != nil {
		// Rollback: remove symbols that failed to subscribe
		c.subMu.Lock()
		for _, sym := range newSymbols {
			delete(c.subscribedSymbols, sym)
			delete(c.lastActive, sym)
		}
		c.subMu.Unlock()
		return err
	}

	return nil
}

// subscribeToSymbols sends subscription requests for given symbols in batches
func (c *WSClient) subscribeToSymbols(symbols []string) error {
	for i := 0; i < len(symbols); i += subscriptionBatchSize {
		end := i + subscriptionBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		topics := make([]string, len(batch))
		for j, symbol := range batch {
			topics[j] = fmt.Sprintf("kline.1.%s", symbol)
		}

		sub := wsSubscription{
			Op:   "subscribe",
			Args: topics,
		}

		// Get connection with read lock
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return fmt.Errorf("connection lost during subscription")
		}

		// Use writeMu to prevent concurrent writes
		c.writeMu.Lock()
		err := conn.WriteJSON(sub)
		c.writeMu.Unlock()

		if err != nil {
			return fmt.Errorf("write subscription: %w", err)
		}

		log.Debug().
			Int("batch_num", i/subscriptionBatchSize+1).
			Int("symbols_count", len(batch)).
			Strs("symbols", batch).
			Msg("subscribed to new kline batch")

		// Small delay between batches to avoid rate limiting
		if end < len(symbols) {
			time.Sleep(batchDelay)
		}
	}

	return nil
}

// GetSubscribedCount returns the number of currently subscribed symbols
func (c *WSClient) GetSubscribedCount() int {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	return len(c.subscribedSymbols)
}

// GetSubscribedSymbols returns a copy of currently subscribed symbols
// This is useful for fetching ticker data for all active symbols
func (c *WSClient) GetSubscribedSymbols() []string {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	symbols := make([]string, 0, len(c.subscribedSymbols))
	for sym := range c.subscribedSymbols {
		symbols = append(symbols, sym)
	}
	return symbols
}

// CleanupInactive unsubscribes from symbols that haven't been in top weak list
// for longer than maxInactiveTime. Returns the number of removed subscriptions.
// Note: BTCUSDT is never removed as it's required for RS calculation.
func (c *WSClient) CleanupInactive(maxInactiveTime time.Duration) int {
	if !c.connected.Load() {
		return 0
	}

	now := time.Now()

	// Find inactive symbols
	c.subMu.Lock()
	var toRemove []string
	for sym, lastTime := range c.lastActive {
		// Never remove BTCUSDT - required for RS calculation
		if sym == "BTCUSDT" {
			continue
		}

		if now.Sub(lastTime) > maxInactiveTime {
			toRemove = append(toRemove, sym)
		}
	}

	// Remove from tracking maps
	for _, sym := range toRemove {
		delete(c.subscribedSymbols, sym)
		delete(c.lastActive, sym)
	}
	c.subMu.Unlock()

	if len(toRemove) == 0 {
		return 0
	}

	// Send unsubscribe requests
	if err := c.unsubscribeFromSymbols(toRemove); err != nil {
		log.Warn().Err(err).Int("count", len(toRemove)).Msg("failed to unsubscribe from inactive symbols")
		// Continue anyway - symbols are already removed from tracking
	}

	log.Info().
		Int("removed", len(toRemove)).
		Dur("max_inactive", maxInactiveTime).
		Msg("cleaned up inactive subscriptions")

	return len(toRemove)
}

// unsubscribeFromSymbols sends unsubscribe requests for given symbols in batches
func (c *WSClient) unsubscribeFromSymbols(symbols []string) error {
	for i := 0; i < len(symbols); i += subscriptionBatchSize {
		end := i + subscriptionBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		topics := make([]string, len(batch))
		for j, symbol := range batch {
			topics[j] = fmt.Sprintf("kline.1.%s", symbol)
		}

		unsub := wsSubscription{
			Op:   "unsubscribe",
			Args: topics,
		}

		// Get connection with read lock
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return fmt.Errorf("connection lost during unsubscription")
		}

		// Use writeMu to prevent concurrent writes
		c.writeMu.Lock()
		err := conn.WriteJSON(unsub)
		c.writeMu.Unlock()

		if err != nil {
			return fmt.Errorf("write unsubscription: %w", err)
		}

		log.Debug().
			Int("batch_num", i/subscriptionBatchSize+1).
			Int("symbols_count", len(batch)).
			Msg("unsubscribed from kline batch")

		if end < len(symbols) {
			time.Sleep(batchDelay)
		}
	}

	return nil
}
