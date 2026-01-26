package strategy

import (
	"testing"
	"time"

	"github.com/islamtagirov/millionaire-bot/internal/models"
)

func TestCalculateScore(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name        string
		rs          float64
		volumeRatio float64
		supportAge  time.Duration
		wantMin     int
		wantMax     int
	}{
		{
			name:        "weak asset with high volume",
			rs:          -5.5,
			volumeRatio: 3.5,
			supportAge:  45 * time.Minute,
			wantMin:     70,
			wantMax:     100,
		},
		{
			name:        "moderate weakness",
			rs:          -3.5,
			volumeRatio: 2.2,
			supportAge:  35 * time.Minute,
			wantMin:     50,
			wantMax:     70,
		},
		{
			name:        "low score",
			rs:          -2.0,
			volumeRatio: 1.8,
			supportAge:  15 * time.Minute,
			wantMin:     0,
			wantMax:     30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			support := &models.SupportLevel{
				Price:     100.0,
				Timestamp: time.Now().Add(-tt.supportAge),
			}

			candle := models.Candle{
				Symbol: "TESTUSDT",
				Close:  99.0,
			}

			score := engine.calculateScore(tt.rs, tt.volumeRatio, support, candle)

			if score < tt.wantMin || score > tt.wantMax {
				t.Errorf("calculateScore() = %v, want between %v and %v", score, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestDetectSupport(t *testing.T) {
	engine := NewEngine()

	// Create test candles with a clear fractal low
	baseTime := time.Now()
	candles := []models.Candle{
		{Low: 102.0, Timestamp: baseTime.Add(-60 * time.Minute)},
		{Low: 101.0, Timestamp: baseTime.Add(-59 * time.Minute)},
		{Low: 100.0, Timestamp: baseTime.Add(-58 * time.Minute)}, // Fractal low
		{Low: 101.5, Timestamp: baseTime.Add(-57 * time.Minute)},
		{Low: 102.5, Timestamp: baseTime.Add(-56 * time.Minute)},
		{Low: 103.0, Timestamp: baseTime.Add(-55 * time.Minute)},
		{Low: 102.0, Timestamp: baseTime.Add(-54 * time.Minute)},
	}

	// Fill the rest to meet minimum requirements
	for i := len(candles); i < SupportLookback; i++ {
		candles = append(candles, models.Candle{
			Low:       101.0 + float64(i%10),
			Timestamp: baseTime.Add(time.Duration(-60+i) * time.Minute),
		})
	}

	support := DetectSupport(candles)

	if support == nil {
		t.Fatal("DetectSupport() returned nil, want support level")
	}

	if support.Price < 99.0 || support.Price > 101.0 {
		t.Errorf("DetectSupport() price = %v, want around 100.0", support.Price)
	}
}

func TestCalculateAvgVolume(t *testing.T) {
	engine := NewEngine()

	candles := []models.Candle{
		{Volume: 100.0},
		{Volume: 200.0},
		{Volume: 300.0},
		{Volume: 400.0},
		{Volume: 500.0},
	}

	avgVolume := engine.calculateAvgVolume(candles)
	expectedAvg := 300.0

	if avgVolume != expectedAvg {
		t.Errorf("calculateAvgVolume() = %v, want %v", avgVolume, expectedAvg)
	}
}

func TestProcessCandle(t *testing.T) {
	engine := NewEngine()

	// Add BTC candles for RS calculation
	baseTime := time.Now()
	candlesToAdd := MinRSLookback + 10 // Add a bit more than minimum
	for i := 0; i < candlesToAdd; i++ {
		btcCandle := models.Candle{
			Symbol:    "BTCUSDT",
			Timestamp: baseTime.Add(time.Duration(-candlesToAdd+i) * time.Minute),
			Open:      50000.0,
			High:      50100.0,
			Low:       49900.0,
			Close:     50000.0 + float64(i*10), // Slight uptrend
			Volume:    1000.0,
		}
		engine.ProcessCandle(btcCandle)
	}

	// Check BTC candles stored
	if len(engine.btcCandles) != candlesToAdd {
		t.Errorf("BTC candles count = %v, want %v", len(engine.btcCandles), candlesToAdd)
	}

	// Add test asset candles
	for i := 0; i < candlesToAdd; i++ {
		candle := models.Candle{
			Symbol:    "TESTUSDT",
			Timestamp: baseTime.Add(time.Duration(-candlesToAdd+i) * time.Minute),
			Open:      100.0,
			High:      101.0,
			Low:       99.0,
			Close:     100.0 - float64(i)*0.1, // Downtrend
			Volume:    500.0,
		}
		engine.ProcessCandle(candle)
	}

	// Check asset candles stored
	if len(engine.candleCache["TESTUSDT"]) != candlesToAdd {
		t.Errorf("TESTUSDT candles count = %v, want %v", len(engine.candleCache["TESTUSDT"]), candlesToAdd)
	}
}
