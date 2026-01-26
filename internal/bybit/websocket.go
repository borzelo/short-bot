package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

type WSClient struct {
	url        string
	conn       *websocket.Conn
	mu         sync.RWMutex
	candleChan chan models.Candle
	symbols    []string
	reconnect  chan struct{}
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

func NewWSClient(url string, symbols []string) *WSClient {
	return &WSClient{
		url:        url,
		candleChan: make(chan models.Candle, 1000),
		symbols:    symbols,
		reconnect:  make(chan struct{}, 1),
	}
}

func (c *WSClient) Connect(ctx context.Context) error {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
		ReadBufferSize:   8192,
		WriteBufferSize:  8192,
	}

	log.Info().Str("url", c.url).Msg("connecting to ByBit WebSocket")

	conn, resp, err := dialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w (status: %v)", err, resp)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	// Set read deadline and pong handler
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	log.Info().Msg("WebSocket connected successfully")

	// Subscribe to all symbols
	if err := c.subscribe(); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	// Start ping routine
	go c.pingRoutine(ctx)

	// Start read routine
	go c.readRoutine(ctx)

	return nil
}

func (c *WSClient) subscribe() error {
	// ByBit allows max 10 subscriptions per request, batch them
	batchSize := 10
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

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return fmt.Errorf("connection not established")
		}

		if err := conn.WriteJSON(sub); err != nil {
			return fmt.Errorf("write subscription: %w", err)
		}

		log.Info().
			Int("batch_num", i/batchSize+1).
			Int("symbols_count", len(batch)).
			Msg("subscribed to kline batch")

		// Small delay between batches
		time.Sleep(100 * time.Millisecond)
	}

	log.Info().Int("total_symbols", len(c.symbols)).Msg("all subscriptions completed")
	return nil
}

func (c *WSClient) pingRoutine(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				continue
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Error().Err(err).Msg("ping failed, triggering reconnect")
				c.triggerReconnect()
				return
			}
		}
	}
}

func (c *WSClient) readRoutine(ctx context.Context) {
	defer func() {
		log.Warn().Msg("read routine exited, triggering reconnect")
		c.triggerReconnect()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				return
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error().Err(err).Msg("read message error")
				return
			}

			// Parse kline message
			var klineResp wsKlineResponse
			if err := json.Unmarshal(message, &klineResp); err != nil {
				// Skip non-kline messages (pings, subscription confirmations, etc.)
				continue
			}

			// Only process confirmed (closed) candles
			if klineResp.Topic == "" || len(klineResp.Data) == 0 {
				continue
			}

			for _, k := range klineResp.Data {
				if !k.Confirm {
					continue
				}

				// Extract symbol from topic "kline.1.BTCUSDT"
				symbol := ""
				fmt.Sscanf(klineResp.Topic, "kline.1.%s", &symbol)

				var open, high, low, close, volume float64
				fmt.Sscanf(k.Open, "%f", &open)
				fmt.Sscanf(k.High, "%f", &high)
				fmt.Sscanf(k.Low, "%f", &low)
				fmt.Sscanf(k.Close, "%f", &close)
				fmt.Sscanf(k.Volume, "%f", &volume)

				candle := models.Candle{
					Symbol:    symbol,
					Timestamp: time.UnixMilli(k.Start),
					Open:      open,
					High:      high,
					Low:       low,
					Close:     close,
					Volume:    volume,
				}

				select {
				case c.candleChan <- candle:
				case <-ctx.Done():
					return
				default:
					log.Warn().Str("symbol", symbol).Msg("candle channel full, dropping candle")
				}
			}
		}
	}
}

func (c *WSClient) triggerReconnect() {
	select {
	case c.reconnect <- struct{}{}:
	default:
	}
}

func (c *WSClient) GetCandleChannel() <-chan models.Candle {
	return c.candleChan
}

func (c *WSClient) GetReconnectChannel() <-chan struct{} {
	return c.reconnect
}

func (c *WSClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown")
		c.conn.WriteMessage(websocket.CloseMessage, closeMsg)
		c.conn.Close()
		c.conn = nil
	}
}
