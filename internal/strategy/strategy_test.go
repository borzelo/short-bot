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
		fundingRate float64
		closePos    float64
		wantMin     int
		wantMax     int
	}{
		{
			name:        "weak asset with high volume",
			rs:          -5.5,
			volumeRatio: 3.5,
			supportAge:  45 * time.Minute,
			fundingRate: 0.02,
			closePos:    0.05,
			wantMin:     70,
			wantMax:     100,
		},
		{
			name:        "moderate weakness",
			rs:          -3.5,
			volumeRatio: 2.2,
			supportAge:  35 * time.Minute,
			fundingRate: 0.005,
			closePos:    0.2,
			wantMin:     40,
			wantMax:     70,
		},
		{
			name:        "low score - not weak enough",
			rs:          -2.0,
			volumeRatio: 1.8,
			supportAge:  15 * time.Minute,
			fundingRate: 0.001,
			closePos:    0.25,
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
				Open:   100.0,
				Close:  99.0,
			}

			score := engine.calculateScore(tt.rs, tt.volumeRatio, support, candle, tt.fundingRate, tt.closePos)

			if score < tt.wantMin || score > tt.wantMax {
				t.Errorf("calculateScore() = %v, want between %v and %v", score, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestCalculateScoreV150(t *testing.T) {
	engine := NewEngine()

	tests := []struct {
		name               string
		rs                 float64
		volumeRatio        float64
		volumeZScore       float64
		deltaOI            float64
		isAggressiveShort  bool
		isSmartPumpRollover bool
		supportAge         time.Duration
		fundingRate        float64
		closePos           float64
		wantMin            int
		wantMax            int
	}{
		{
			name:               "aggressive short with high z-score",
			rs:                 -5.5,
			volumeRatio:        3.5,
			volumeZScore:       4.0,
			deltaOI:            0.05,
			isAggressiveShort:  true,
			isSmartPumpRollover: false,
			supportAge:         45 * time.Minute,
			fundingRate:        0.02,
			closePos:           0.05,
			wantMin:            85,
			wantMax:            100,
		},
		{
			name:               "smart pump rollover",
			rs:                 -4.0,
			volumeRatio:        2.5,
			volumeZScore:       3.5,
			deltaOI:            0.01,
			isAggressiveShort:  false,
			isSmartPumpRollover: true,
			supportAge:         30 * time.Minute,
			fundingRate:        0.015,
			closePos:           0.08,
			wantMin:            70,
			wantMax:            100,
		},
		{
			name:               "standard signal without v1.5 bonuses",
			rs:                 -3.5,
			volumeRatio:        2.0,
			volumeZScore:       2.0,
			deltaOI:            0.01,
			isAggressiveShort:  false,
			isSmartPumpRollover: false,
			supportAge:         25 * time.Minute,
			fundingRate:        0.005,
			closePos:           0.2,
			wantMin:            40,
			wantMax:            70,
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
				Open:   100.0,
				Close:  99.0,
			}

			score := engine.calculateScoreV150(
				tt.rs, tt.volumeRatio, support, candle, tt.fundingRate, tt.closePos,
				tt.volumeZScore, tt.deltaOI, tt.isAggressiveShort, tt.isSmartPumpRollover,
			)

			if score < tt.wantMin || score > tt.wantMax {
				t.Errorf("calculateScoreV150() = %v, want between %v and %v", score, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestDetectSupport(t *testing.T) {
	// Create test candles with a clear fractal low
	baseTime := time.Now()
	candles := []models.Candle{
		{Low: 102.0, High: 103.0, Close: 102.5, Timestamp: baseTime.Add(-60 * time.Minute)},
		{Low: 101.0, High: 102.0, Close: 101.5, Timestamp: baseTime.Add(-59 * time.Minute)},
		{Low: 100.0, High: 101.0, Close: 100.5, Timestamp: baseTime.Add(-58 * time.Minute)}, // Fractal low
		{Low: 101.5, High: 102.5, Close: 102.0, Timestamp: baseTime.Add(-57 * time.Minute)},
		{Low: 102.5, High: 103.5, Close: 103.0, Timestamp: baseTime.Add(-56 * time.Minute)},
		{Low: 103.0, High: 104.0, Close: 103.5, Timestamp: baseTime.Add(-55 * time.Minute)},
		{Low: 102.0, High: 103.0, Close: 102.5, Timestamp: baseTime.Add(-54 * time.Minute)},
	}

	// Fill the rest to meet minimum requirements
	for i := len(candles); i < SupportLookback; i++ {
		candles = append(candles, models.Candle{
			Low:       101.0 + float64(i%10),
			High:      102.0 + float64(i%10),
			Close:     101.5 + float64(i%10),
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

	// Create a RingBuffer with test data
	buffer := NewRingBuffer(10)
	volumes := []float64{100.0, 200.0, 300.0, 400.0, 500.0}
	for _, vol := range volumes {
		buffer.Push(models.Candle{Volume: vol})
	}

	avgVolume := engine.calculateAvgVolume(buffer)
	expectedAvg := 300.0

	if avgVolume != expectedAvg {
		t.Errorf("calculateAvgVolume() = %v, want %v", avgVolume, expectedAvg)
	}
}

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer(5)

	// Test pushing elements
	for i := 0; i < 7; i++ {
		rb.Push(models.Candle{Close: float64(i)})
	}

	// Buffer should only hold last 5 elements
	if rb.Len() != 5 {
		t.Errorf("RingBuffer.Len() = %v, want 5", rb.Len())
	}

	// Check oldest element (should be 2, since 0,1 were overwritten)
	if rb.Get(0).Close != 2 {
		t.Errorf("RingBuffer.Get(0).Close = %v, want 2", rb.Get(0).Close)
	}

	// Check newest element
	if rb.Last().Close != 6 {
		t.Errorf("RingBuffer.Last().Close = %v, want 6", rb.Last().Close)
	}

	// Test LastN
	lastThree := rb.LastN(3)
	if len(lastThree) != 3 {
		t.Errorf("RingBuffer.LastN(3) len = %v, want 3", len(lastThree))
	}
	if lastThree[0].Close != 4 || lastThree[2].Close != 6 {
		t.Errorf("RingBuffer.LastN(3) = %v, want [4,5,6]", lastThree)
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

	// Check BTC candles stored in btcBuffer
	if engine.btcBuffer.Len() != candlesToAdd {
		t.Errorf("BTC candles count = %v, want %v", engine.btcBuffer.Len(), candlesToAdd)
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

	// Check asset candles stored in candleCache
	buffer, exists := engine.candleCache["TESTUSDT"]
	if !exists {
		t.Fatal("TESTUSDT not found in candleCache")
	}
	if buffer.Len() != candlesToAdd {
		t.Errorf("TESTUSDT candles count = %v, want %v", buffer.Len(), candlesToAdd)
	}
}

func TestCalculateOIDivergence(t *testing.T) {
	engine := NewEngine()

	symbol := "TESTUSDT"

	// Setup: Add OI snapshot from 15+ minutes ago
	engine.oiSnapshots[symbol] = &models.OISnapshot{
		OpenInterest: 1000000,
		Timestamp:    time.Now().Add(-20 * time.Minute),
	}

	tests := []struct {
		name              string
		currentOI         float64
		candleOpen        float64
		candleClose       float64
		wantAggressiveShort bool
		wantLongExit      bool
	}{
		{
			name:              "aggressive short - OI up, price down",
			currentOI:         1030000, // +3%
			candleOpen:        100.0,
			candleClose:       98.0, // Price down
			wantAggressiveShort: true,
			wantLongExit:      false,
		},
		{
			name:              "long exit - OI down, price down",
			currentOI:         970000, // -3%
			candleOpen:        100.0,
			candleClose:       98.0, // Price down
			wantAggressiveShort: false,
			wantLongExit:      true,
		},
		{
			name:              "neutral - OI stable",
			currentOI:         1010000, // +1% (below threshold)
			candleOpen:        100.0,
			candleClose:       98.0,
			wantAggressiveShort: false,
			wantLongExit:      false,
		},
		{
			name:              "price up - no divergence signals",
			currentOI:         1030000, // +3%
			candleOpen:        98.0,
			candleClose:       100.0, // Price up
			wantAggressiveShort: false,
			wantLongExit:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update current OI
			engine.ticker24hStats[symbol] = &models.Ticker24hStats{
				OpenInterest: tt.currentOI,
			}

			candle := models.Candle{
				Symbol: symbol,
				Open:   tt.candleOpen,
				Close:  tt.candleClose,
			}

			_, isAggressiveShort, isLongExit := engine.calculateOIDivergence(symbol, candle)

			if isAggressiveShort != tt.wantAggressiveShort {
				t.Errorf("isAggressiveShort = %v, want %v", isAggressiveShort, tt.wantAggressiveShort)
			}
			if isLongExit != tt.wantLongExit {
				t.Errorf("isLongExit = %v, want %v", isLongExit, tt.wantLongExit)
			}
		})
	}
}

func TestCalculateVolumeZScore(t *testing.T) {
	engine := NewEngine()

	// Create buffer with historical volumes
	buffer := NewRingBuffer(30)
	// Add 24 candles with avg volume ~1000, stddev ~100
	volumes := []float64{
		950, 1050, 980, 1020, 990, 1010, 970, 1030, 960, 1040,
		955, 1045, 985, 1015, 995, 1005, 975, 1025, 965, 1035,
		940, 1060, 970, 1030,
	}
	for _, vol := range volumes {
		buffer.Push(models.Candle{Volume: vol})
	}

	// With the test data above: mean ≈ 1000, stdDev ≈ 35
	// Z-Score = (value - mean) / stdDev
	tests := []struct {
		name          string
		currentVolume float64
		wantZScoreMin float64
		wantZScoreMax float64
	}{
		{
			name:          "normal volume",
			currentVolume: 1000, // Z = (1000-1000)/35 ≈ 0
			wantZScoreMin: -1.0,
			wantZScoreMax: 1.0,
		},
		{
			name:          "high volume anomaly",
			currentVolume: 1300, // Z = (1300-1000)/35 ≈ 8.5
			wantZScoreMin: 7.0,
			wantZScoreMax: 10.0,
		},
		{
			name:          "very high volume",
			currentVolume: 1500, // Z = (1500-1000)/35 ≈ 14.3
			wantZScoreMin: 12.0,
			wantZScoreMax: 16.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zScore, _, _ := engine.calculateVolumeZScore(buffer, tt.currentVolume)

			if zScore < tt.wantZScoreMin || zScore > tt.wantZScoreMax {
				t.Errorf("calculateVolumeZScore() z-score = %v, want between %v and %v",
					zScore, tt.wantZScoreMin, tt.wantZScoreMax)
			}
		})
	}
}
