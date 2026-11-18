package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"lamina/internal/snapshot"
)

// The consumer registry is durable: acknowledgements are what let retention
// distinguish a slow but healthy consumer from a dead one across restarts. If
// the registry were only in memory, a source restart would forget a live
// consumer and could prune the history it still needs.
const (
	consumerFile = "consumers.json"
	maxConsumers = 1024
)

type consumerFileFormat struct {
	Format    int                    `json:"format"`
	Consumers map[string]Consumption `json:"consumers"`
}

func (d *DB) loadConsumers() error {
	b, err := os.ReadFile(filepath.Join(d.dir, consumerFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var file consumerFileFormat
	if err = json.Unmarshal(b, &file); err != nil {
		return fmt.Errorf("consumer registry: %w", err)
	}
	if file.Format != 1 {
		return fmt.Errorf("unsupported consumer registry format")
	}
	if file.Consumers == nil {
		file.Consumers = map[string]Consumption{}
	}
	for name, entry := range file.Consumers {
		d.consumers[name] = entry
	}
	return nil
}

func (d *DB) saveConsumers() error {
	file := consumerFileFormat{Format: 1, Consumers: d.consumers}
	b, err := json.Marshal(file)
	if err != nil {
		return err
	}
	return snapshot.Atomic(filepath.Join(d.dir, consumerFile), b)
}

// retentionFile keeps the operator's trade-off durable even between
// checkpoints; the checkpoint also carries it, but an unclean shutdown must not
// silently restore unbounded retention.
const retentionFile = "retention.json"

type retentionFormat struct {
	Format        int    `json:"format"`
	MaxBytes      int64  `json:"max_bytes"`
	MaxLagRecords uint64 `json:"max_lag_records"`
	MaxLagSeconds uint64 `json:"max_lag_seconds"`
}

func (d *DB) loadRetention() error {
	b, err := os.ReadFile(filepath.Join(d.dir, retentionFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var file retentionFormat
	if err = json.Unmarshal(b, &file); err != nil {
		return fmt.Errorf("retention policy: %w", err)
	}
	if file.Format != 1 || file.MaxBytes < 0 {
		return fmt.Errorf("invalid retention policy")
	}
	d.retention = RetentionPolicy{MaxBytes: file.MaxBytes, MaxLagRecords: file.MaxLagRecords, MaxLagSeconds: file.MaxLagSeconds}
	return nil
}

func (d *DB) saveRetention() error {
	file := retentionFormat{Format: 1, MaxBytes: d.retention.MaxBytes, MaxLagRecords: d.retention.MaxLagRecords, MaxLagSeconds: d.retention.MaxLagSeconds}
	b, err := json.Marshal(file)
	if err != nil {
		return err
	}
	return snapshot.Atomic(filepath.Join(d.dir, retentionFile), b)
}
