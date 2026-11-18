package engine

// Retention is the source-side resource policy. It is expressed in the two
// costs a user can actually trade against each other: disk retained for
// consumers, and how far a consumer may fall behind before it must
// reinitialize. A zero value means "unbounded" for that dimension.
type Retention struct {
	MaxBytes       int64  `json:"max_bytes"`
	MaxLagRecords  uint64 `json:"max_lag_records"`
	MaxLagSeconds  uint64 `json:"max_lag_seconds"`
	Pinned         bool   `json:"pinned"`
	OverBudget     bool   `json:"over_budget"`
	HistoryBase    uint64 `json:"history_base"`
	RetainedBytes  int64  `json:"retained_bytes"`
	OldestSequence uint64 `json:"oldest_sequence"`
	Head           uint64 `json:"head"`
}

// Consumption is the durable-per-session acknowledgement state of one named
// downstream consumer. The watermark is the last source sequence the consumer
// reports it has completely applied; it is never the "next" sequence to read.
type Consumption struct {
	Consumer      string `json:"consumer"`
	Watermark     uint64 `json:"watermark"`
	Digest        string `json:"digest,omitempty"`
	Acknowledged  string `json:"acknowledged"`
	AgeSeconds    uint64 `json:"age_seconds"`
	BehindRecords uint64 `json:"behind_records"`
}

// RetentionPolicy is the configurable subset, without computed fields.
type RetentionPolicy struct {
	MaxBytes      int64  `json:"max_bytes"`
	MaxLagRecords uint64 `json:"max_lag_records"`
	MaxLagSeconds uint64 `json:"max_lag_seconds"`
}

const (
	defaultMaxBytes      = 64 << 20
	minMaxBytes          = 1 << 10
	maxMaxBytes          = 8 << 30
	defaultMaxLagRecords = 10000
	maxConsumerIDLength  = 64
)
