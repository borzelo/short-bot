package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
	"github.com/rs/zerolog/log"
)

type Notifier struct {
	botToken string
	chatID   string
	client   *http.Client
}

type sendMessageRequest struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

func NewNotifier(botToken, chatID string) *Notifier {
	return &Notifier{
		botToken: botToken,
		chatID:   chatID,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (n *Notifier) SendSignal(signal *models.Signal) error {
	// Log start of sending
	log.Info().
		Str("symbol", signal.Symbol).
		Int("score", signal.ScoreTotal).
		Float64("price", signal.PriceTrigger).
		Float64("level_broken", signal.LevelBroken).
		Msg("preparing to send telegram notification")

	message := n.formatMessage(signal)

	payload := sendMessageRequest{
		ChatID:    n.chatID,
		Text:      message,
		ParseMode: "Markdown",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Error().Err(err).Str("symbol", signal.Symbol).Msg("failed to marshal telegram payload")
		return fmt.Errorf("marshal payload: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", n.botToken)

	log.Debug().
		Str("symbol", signal.Symbol).
		Str("chat_id", n.chatID).
		Int("payload_size", len(jsonData)).
		Msg("sending telegram request")

	resp, err := n.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Error().Err(err).Str("symbol", signal.Symbol).Msg("failed to send telegram request")
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read response body for debugging
		var respBody bytes.Buffer
		respBody.ReadFrom(resp.Body)
		log.Error().
			Int("status_code", resp.StatusCode).
			Str("response", respBody.String()).
			Str("symbol", signal.Symbol).
			Msg("telegram API returned error")
		return fmt.Errorf("telegram api error: status %d, response: %s", resp.StatusCode, respBody.String())
	}

	log.Info().
		Str("symbol", signal.Symbol).
		Int("score", signal.ScoreTotal).
		Float64("price", signal.PriceTrigger).
		Msg("telegram notification sent successfully")

	return nil
}

func (n *Notifier) formatMessage(signal *models.Signal) string {
	// Get metadata
	volumeRatio := 0.0
	supportAge := 0.0

	if val, ok := signal.Meta["volume_ratio"].(float64); ok {
		volumeRatio = val
	}
	if val, ok := signal.Meta["support_age_minutes"].(float64); ok {
		supportAge = val
	}

	// Determine probability text based on score
	probability := ""
	switch {
	case signal.ScoreTotal >= 80:
		probability = "Высокая вероятность падения"
	case signal.ScoreTotal >= 60:
		probability = "Средняя вероятность падения"
	case signal.ScoreTotal >= 40:
		probability = "Умеренная вероятность падения"
	default:
		probability = "Низкая вероятность падения"
	}

	// Format message in Russian
	message := fmt.Sprintf(`🚨 *СИГНАЛ: ПРОБОЙ УРОВНЯ* 🚨

Тикер: #%s
Цена: %.4f (Пробит уровень: %.4f)

📉 *Факторы слабости:*
• Относит. сила (RS): %.2f%% (Слабее рынка)
• Объем при пробое: x%.1f от среднего

🛠 *Технический анализ:*
• Время жизни уровня: %.0f мин
• Объем пробоя: %.2f

🏆 *ОБЩИЙ БАЛЛ: %d/100*
*(%s)*
`,
		signal.Symbol,
		signal.PriceTrigger,
		signal.LevelBroken,
		signal.ScoreRS,
		volumeRatio,
		supportAge,
		signal.BreakdownVolume,
		signal.ScoreTotal,
		probability,
	)

	return message
}
