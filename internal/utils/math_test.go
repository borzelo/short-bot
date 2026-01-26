package utils

import (
	"math"
	"testing"
)

func TestMean(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{
			name:   "simple average",
			values: []float64{1, 2, 3, 4, 5},
			want:   3.0,
		},
		{
			name:   "single value",
			values: []float64{42},
			want:   42.0,
		},
		{
			name:   "empty slice",
			values: []float64{},
			want:   0.0,
		},
		{
			name:   "negative values",
			values: []float64{-2, -1, 0, 1, 2},
			want:   0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Mean(tt.values)
			if got != tt.want {
				t.Errorf("Mean() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStdDev(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
		delta  float64 // Acceptable difference due to floating point
	}{
		{
			name:   "known stddev",
			values: []float64{2, 4, 4, 4, 5, 5, 7, 9},
			want:   2.138, // Sample stddev
			delta:  0.01,
		},
		{
			name:   "single value",
			values: []float64{42},
			want:   0.0,
			delta:  0.001,
		},
		{
			name:   "empty slice",
			values: []float64{},
			want:   0.0,
			delta:  0.001,
		},
		{
			name:   "two values",
			values: []float64{10, 20},
			want:   7.071, // sqrt((10-15)^2 + (20-15)^2) / 1) = sqrt(50)
			delta:  0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StdDev(tt.values)
			if math.Abs(got-tt.want) > tt.delta {
				t.Errorf("StdDev() = %v, want %v (±%v)", got, tt.want, tt.delta)
			}
		})
	}
}

func TestZScore(t *testing.T) {
	tests := []struct {
		name   string
		value  float64
		mean   float64
		stdDev float64
		want   float64
	}{
		{
			name:   "positive z-score",
			value:  15,
			mean:   10,
			stdDev: 2.5,
			want:   2.0,
		},
		{
			name:   "negative z-score",
			value:  5,
			mean:   10,
			stdDev: 2.5,
			want:   -2.0,
		},
		{
			name:   "zero z-score (at mean)",
			value:  10,
			mean:   10,
			stdDev: 2.5,
			want:   0.0,
		},
		{
			name:   "zero stddev (division by zero protection)",
			value:  15,
			mean:   10,
			stdDev: 0,
			want:   0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ZScore(tt.value, tt.mean, tt.stdDev)
			if got != tt.want {
				t.Errorf("ZScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateVolumeZScore(t *testing.T) {
	tests := []struct {
		name              string
		currentVolume     float64
		historicalVolumes []float64
		wantZScoreMin     float64
		wantZScoreMax     float64
	}{
		{
			name:              "normal volume",
			currentVolume:     1000,
			historicalVolumes: []float64{900, 1000, 1100, 950, 1050, 980, 1020},
			wantZScoreMin:     -1.0,
			wantZScoreMax:     1.0,
		},
		{
			name:              "high volume anomaly",
			currentVolume:     1500,
			historicalVolumes: []float64{900, 1000, 1100, 950, 1050, 980, 1020},
			wantZScoreMin:     2.0,
			wantZScoreMax:     10.0,
		},
		{
			name:              "insufficient data",
			currentVolume:     1000,
			historicalVolumes: []float64{900},
			wantZScoreMin:     0.0,
			wantZScoreMax:     0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zScore, _, _ := CalculateVolumeZScore(tt.currentVolume, tt.historicalVolumes)
			if zScore < tt.wantZScoreMin || zScore > tt.wantZScoreMax {
				t.Errorf("CalculateVolumeZScore() z-score = %v, want between %v and %v",
					zScore, tt.wantZScoreMin, tt.wantZScoreMax)
			}
		})
	}
}

func TestDistanceFromHigh(t *testing.T) {
	tests := []struct {
		name         string
		currentPrice float64
		high24h      float64
		want         float64
	}{
		{
			name:         "at high",
			currentPrice: 100,
			high24h:      100,
			want:         0.0,
		},
		{
			name:         "5% below high",
			currentPrice: 95,
			high24h:      100,
			want:         0.05,
		},
		{
			name:         "10% below high",
			currentPrice: 90,
			high24h:      100,
			want:         0.10,
		},
		{
			name:         "zero high (protection)",
			currentPrice: 95,
			high24h:      0,
			want:         0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DistanceFromHigh(tt.currentPrice, tt.high24h)
			if math.Abs(got-tt.want) > 0.001 {
				t.Errorf("DistanceFromHigh() = %v, want %v", got, tt.want)
			}
		})
	}
}
