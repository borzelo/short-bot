package training

// Configuration constants for data mining (same as strategy package)
const (
	MaxCandlesInMemory = 240 // 4 hours of 1-minute candles
	RSLookbackMinutes  = 60  // 1 hour for RS calculation
	MinRSLookback      = 30  // Minimum 30 minutes to start analysis
	SupportLookback    = 60  // 60 minutes for support detection
	VolumeAvgWindow    = 20  // 20 candles for volume average
)

// DATA MINING THRESHOLDS (v1.9.0)
// These are MORE PERMISSIVE than trading thresholds to collect diverse training data
// Goal: Let the ML model learn from both good and bad signals
const (
	// RS Threshold: 0.0 (neutral) instead of -1.2
	// Rationale: Model needs to see assets at par or slightly stronger than BTC
	//            to learn why they shouldn't be shorted
	RSWeakThreshold     = 0.0  // Allow assets equal to or weaker than BTC
	RSVeryWeakThreshold = -5.0 // Keep same for extreme weakness detection

	// Volume Threshold: 1.2x instead of 1.8x
	// Rationale: Model needs examples of "weak" breakdowns without volume
	//            to learn they are false signals
	VolumeMinRatio    = 1.2 // Lower threshold - collect weak volume breakdowns
	VolumeMediumRatio = 2.0 // Keep same
	VolumeHighRatio   = 3.0 // Keep same

	// Other thresholds (same as trading)
	ClosePositionMax    = 0.3  // Maximum close position (bottom 30%)
	ClosePositionMaxPct = 0.3  // Alias for filter clarity
	ClosePositionStrong = 0.1  // Strong close position for bonus
	SupportAgeBonus     = 20.0 // Minutes for support age bonus

	// Funding Rate: -0.03 instead of -0.015
	// Rationale: Allow slightly crowded shorts so model learns squeeze scenarios
	FundingAntiSqueeze = -0.03 // More permissive - allow crowded shorts
	FundingPositive    = 0.01  // Keep same

	// Volatility: Same as trading (3% minimum)
	MinVolatility24h = 0.03 // Minimum 24h volatility to avoid dead coins
	VolatilityNDRMin = 0.03 // Alias: NDR (Net Daily Range) minimum

	// Oversold Filter: -40% instead of -25%
	// Rationale: Collect data from heavily dumped coins to teach "Dead Cat Bounce" pattern
	OversoldThreshold24h = -0.40 // More permissive - allow crashed assets

	// MA Extension: 8% instead of 4%
	// Rationale: Collect data when price is far from mean to teach mean reversion
	MAExtensionThreshold = 0.08 // More permissive - allow extended assets

	// Dynamic Support Age thresholds (same as trading)
	HighVolatilityThreshold1h = -0.02 // -2% in 1h = high volatility
	HighVolatilitySupportAge  = 45.0  // 45 minutes during high volatility
	DefaultSupportAge         = 30.0  // 30 minutes normal conditions
	SupportMinAge             = 60    // Minimum support age (minutes)
	SupportMinAgeMultiTouch   = 30    // Reduced minimum if 3+ touches

	// SMA calculation (same as trading)
	SMAWindow     = 200 // 200 candles for SMA
	SMAMinCandles = 60  // Minimum candles for reliable SMA

	// Pump filters (same as trading)
	MaxPriceGain24h       = 0.30  // 30% max pump
	MaxPumpThreshold      = 0.30  // Alias for filter clarity
	SmartPumpThreshold    = 0.15  // 15% triggers smart filter (was 0.20 in old version)
	SmartPumpNearHighDist = 0.03  // 3% from high = near high
	SmartPumpRolloverDist = 0.05  // 5% from high = rollover confirmed (was 0.10 in old version)
	PumpRolloverThreshold = 0.05  // 5% for legacy pump rollover
	SmartPumpRolloverBonus = 20   // Bonus points for smart rollover
	PumpRolloverBonus     = 15    // Legacy pump rollover bonus

	// OI Divergence (same as trading)
	OISnapshotAgeMinutes   = 15.0 // Snapshot age for OI delta
	OIDivergenceThreshold  = 0.02 // 2% OI change threshold
	OIAggressiveShortBonus = 15   // Bonus for aggressive short
	OILongExitPenalty      = -50  // Block long exits

	// Volume Z-Score (same as trading)
	VolumeZScoreWindow    = 50  // 50 candles for Z-Score
	VolumeZScoreThreshold = 3.0 // Z > 3.0 = extreme anomaly
)
