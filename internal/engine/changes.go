package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/changelog"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ChangePage is one commit-bounded page of the retained feed. Pages never split
// a record, so a transaction's writes always appear together. Cursor is the
// last sequence the caller may treat as applied, and Digest identifies that
// exact sequence so a consumer can tell a real continuation from a source that
// was rebuilt and merely reuses the same numbers.
type ChangePage struct {
	SourceID     string           `json:"source_id"`
	Epoch        string           `json:"epoch"`
	Origin       *snapshot.Origin `json:"origin,omitempty"`
	Records      []journal.Record `json:"records"`
	Cursor       uint64           `json:"cursor"`
	CursorDigest string           `json:"cursor_digest"`
	Head         uint64           `json:"head"`
	HeadDigest   string           `json:"head_digest"`
	HistoryBase  uint64           `json:"history_base"`
	More         bool             `json:"more"`
	Retention    Retention        `json:"retention"`
}

func (d *DB) retentionView(now time.Time) Retention {
	view := Retention{
		MaxBytes:       d.retention.MaxBytes,
		MaxLagRecords:  d.retention.MaxLagRecords,
		MaxLagSeconds:  d.retention.MaxLagSeconds,
		HistoryBase:    d.archive.Base(),
		RetainedBytes:  d.archive.Bytes(),
		OldestSequence: d.archive.Base(),
		Head:           d.archive.Head(),
	}
	if d.archive.Head() > d.archive.Base() && d.archive.Count() > 0 {
		view.OldestSequence = d.archive.Base() + 1
	}
	if d.retention.MaxBytes > 0 && d.archive.Bytes() > d.retention.MaxBytes {
		view.OverBudget = true
	}
	_, _, pinned := d.prunePlan(now)
	view.Pinned = pinned
	return view
}

func (d *DB) Changes(after uint64, limit int) (ChangePage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	now := time.Now().UTC()
	out := ChangePage{
		SourceID:    d.identity.SourceID,
		Epoch:       d.identity.Epoch,
		Origin:      d.identity.Origin,
		Records:     []journal.Record{},
		Cursor:      after,
		Head:        d.archive.Head(),
		HeadDigest:  d.archive.HeadDigest(),
		HistoryBase: d.archive.Base(),
		Retention:   d.retentionView(now),
	}
	if err := d.usable(); err != nil {
		return out, err
	}
	if after < d.archive.Base() {
		return out, fault("snapshot", "change history begins at %d; consumer must reinitialize from a backup", d.archive.Base())
	}
	if after > d.archive.Head() || limit < 1 || limit > 1000 {
		return out, fault("invalid", "invalid change cursor or limit")
	}
	if after == d.archive.Head() && after != d.archive.Base() {
		digest, _ := d.archive.DigestAt(after)
		out.CursorDigest = digest
		out.HeadDigest = digest
		return out, nil
	}
	page, more := d.archive.RecordsAfter(after, limit)
	out.Records = page
	out.More = more
	if len(page) > 0 {
		out.Cursor = page[len(page)-1].Sequence
		out.CursorDigest = page[len(page)-1].Digest
	} else {
		digest, _ := d.archive.DigestAt(after)
		out.CursorDigest = digest
	}
	out.HeadDigest = d.archive.HeadDigest()
	return out, nil
}

// DigestAt resolves one retained sequence to its lineage digest so a consumer
// can confirm the point it is about to resume from is the point it applied.
func (d *DB) DigestAt(sequence uint64) (string, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return "", false, err
	}
	digest, ok := d.archive.DigestAt(sequence)
	return digest, ok, nil
}

func validateConsumer(name string) error {
	if name == "" || len(name) > maxConsumerIDLength {
		return fault("invalid", "consumer name must be 1..64 characters")
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fault("invalid", "consumer name may contain letters, digits, '.', '-' and '_'")
	}
	return nil
}

// Acknowledge records that consumer has completely applied up to watermark.
// When digest is supplied it must match the retained lineage at that sequence,
// so a consumer cannot acknowledge a position from a different history.
func (d *DB) Acknowledge(consumer string, watermark uint64, digest string) (Consumption, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Consumption{}, err
	}
	if err := validateConsumer(consumer); err != nil {
		return Consumption{}, err
	}
	now := time.Now().UTC()
	if prior, ok := d.consumers[consumer]; ok && watermark < prior.Watermark {
		return Consumption{}, fault("conflict", "watermark cannot move backwards for %s", consumer)
	}
	actual, ok := d.archive.DigestAt(watermark)
	if !ok {
		if watermark < d.archive.Base() {
			return Consumption{}, fault("snapshot", "watermark %d is before retained history %d", watermark, d.archive.Base())
		}
		return Consumption{}, fault("invalid", "watermark beyond retained head")
	}
	if digest != "" && !strings.EqualFold(digest, actual) {
		return Consumption{}, fault("conflict", "watermark digest does not match retained lineage")
	}
	if _, exists := d.consumers[consumer]; !exists && len(d.consumers) >= maxConsumers {
		return Consumption{}, fault("capacity", "consumer registry limit %d reached", maxConsumers)
	}
	entry := Consumption{Consumer: consumer, Watermark: watermark, Digest: actual, Acknowledged: now.Format(time.RFC3339Nano)}
	if d.archive.Head() >= watermark {
		entry.BehindRecords = d.archive.Head() - watermark
	}
	prior, existed := d.consumers[consumer]
	d.consumers[consumer] = entry
	if err := d.saveConsumers(); err != nil {
		if existed {
			d.consumers[consumer] = prior
		} else {
			delete(d.consumers, consumer)
		}
		return Consumption{}, fault("unavailable", "consumer registry: %s", err)
	}
	d.pruneLocked(now)
	return d.consumptionView(entry, now), nil
}

func (d *DB) consumptionView(entry Consumption, now time.Time) Consumption {
	entry.AgeSeconds = uint64(now.Sub(parseStamp(entry.Acknowledged)) / time.Second)
	entry.BehindRecords = d.archive.Head() - entry.Watermark
	return entry
}

func (d *DB) Consumers() ([]Consumption, Retention, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, Retention{}, err
	}
	now := time.Now().UTC()
	out := make([]Consumption, 0, len(d.consumers))
	for _, c := range d.consumers {
		out = append(out, d.consumptionView(c, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Consumer < out[j].Consumer })
	return out, d.retentionView(now), nil
}

// SetRetention changes the source-side trade-off. Lowering the byte budget
// prunes immediately; records still owed to a live consumer below the lag
// window are kept, so the budget can be temporarily exceeded rather than
// silently breaking a healthy consumer.
func (d *DB) SetRetention(maxBytes int64, maxLagRecords, maxLagSeconds uint64) (Retention, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Retention{}, err
	}
	if maxBytes != 0 && (maxBytes < minMaxBytes || maxBytes > maxMaxBytes) {
		return Retention{}, fault("invalid", "max_bytes must be 0 or 1KiB..8GiB")
	}
	if maxLagSeconds > 86400*30 {
		return Retention{}, fault("invalid", "max_lag_seconds exceeds 30 days")
	}
	prior := d.retention
	d.retention = RetentionPolicy{MaxBytes: maxBytes, MaxLagRecords: maxLagRecords, MaxLagSeconds: maxLagSeconds}
	if err := d.saveRetention(); err != nil {
		d.retention = prior
		return Retention{}, fault("unavailable", "retention policy: %s", err)
	}
	d.pruneLocked(time.Now().UTC())
	return d.retentionView(time.Now().UTC()), nil
}

func (d *DB) RetentionState() (Retention, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Retention{}, err
	}
	return d.retentionView(time.Now().UTC()), nil
}

// pruneLocked applies the retention policy. It runs only under the write lock
// and only after durable publication, so it can never delay or fail a commit
// that a consumer has not read. Selection is deterministic: protect the oldest
// live consumer watermark when that consumer is within its lag budget;
// otherwise drop the oldest records until the byte budget is met.
func (d *DB) pruneLocked(now time.Time) {
	cut, _, _ := d.prunePlan(now)
	if cut > d.archive.Base() {
		d.archive.Prune(cut)
	}
}

// prunePlan decides the new history base. protect is the highest watermark of a
// consumer still inside its lag window; a consumer past that window is
// forgotten for retention purposes and must reinitialize if it returns late.
// pinned reports that protecting that consumer is what prevents more pruning
// under the byte budget.
func (d *DB) prunePlan(now time.Time) (cut, protect uint64, pinned bool) {
	base := d.archive.Base()
	head := d.archive.Head()
	if head <= base {
		return base, base, false
	}
	protect = base
	for _, c := range d.consumers {
		if d.retention.MaxLagRecords > 0 && head >= c.Watermark && head-c.Watermark > d.retention.MaxLagRecords {
			continue
		}
		if d.retention.MaxLagSeconds > 0 {
			age := now.Sub(parseStamp(c.Acknowledged))
			if !parseStamp(c.Acknowledged).IsZero() && age > time.Duration(d.retention.MaxLagSeconds)*time.Second {
				continue
			}
		}
		if c.Watermark > protect && c.Watermark < head {
			protect = c.Watermark
		}
	}
	cut = protect
	if d.retention.MaxBytes > 0 {
		// Prune toward half the budget when the budget is exceeded, instead of
		// rewriting the archive on every subsequent commit. With no budget
		// overrun the cut is left at the protected watermark, which is a no-op.
		target := d.retention.MaxBytes
		if d.archive.Bytes() > d.retention.MaxBytes {
			target = d.retention.MaxBytes / 2
			if target < minMaxBytes {
				target = minMaxBytes
			}
		}
		byBytes := d.archive.CutForBytes(target, 0)
		if byBytes > cut {
			if protect > base && protect < head {
				// The byte budget asks for more pruning than a live consumer's
				// watermark allows. Protecting the consumer wins and the excess is
				// reported as pinned/over_budget; tightening max_lag_records is
				// what converts that pressure into a reinitialization.
				pinned = true
			} else {
				// No live consumer needs this history: the budget decides.
				cut = byBytes
			}
		}
	}
	if cut > head {
		cut = head
	}
	return cut, protect, pinned
}

func parseStamp(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return t
}

// openArchive attaches the bounded retained feed to the durability journal. A
// fresh archive is seeded from the journal records still replayable beyond the
// checkpoint, so the feed is complete from the point the database can prove.
func (d *DB) openArchive() error {
	path := filepath.Join(d.dir, "changes.lmc")
	a, err := changelog.Open(path, d.wal.Base, d.wal.BaseDigest(), d.wal.Records)
	if err != nil {
		return fmt.Errorf("change archive: %w", err)
	}
	d.archive = a
	return nil
}

func (d *DB) Identity() snapshot.Identity { return d.identity }

func (d *DB) HeadDigest() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.archive.HeadDigest()
}

// Bootstrap is the reinitialization path for a consumer with no usable cursor.
// It returns a self-consistent relational snapshot at one retained sequence
// together with the lineage digest. A consumer installs both atomically, so its
// first acknowledged watermark is a real, reproducible source point.
type Bootstrap struct {
	SourceID string           `json:"source_id"`
	Epoch    string           `json:"epoch"`
	Origin   *snapshot.Origin `json:"origin,omitempty"`
	Sequence uint64           `json:"sequence"`
	Digest   string           `json:"digest"`
	Catalog  *catalog.Catalog `json:"catalog"`
	Store    *versions.Store  `json:"store"`
	Horizon  uint64           `json:"horizon"`
}

func (d *DB) Bootstrap(at *uint64) (Bootstrap, error, int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Bootstrap{}, err, 0
	}
	sequence := d.archive.Head()
	if at != nil {
		sequence = *at
	}
	if sequence > d.sequence || sequence < d.horizon || sequence < d.archive.Base() {
		return Bootstrap{}, fault("snapshot", "bootstrap sequence outside retained and version history"), 0
	}
	digest, ok := d.archive.DigestAt(sequence)
	if !ok {
		return Bootstrap{}, fault("snapshot", "bootstrap lineage unavailable; wait for a new checkpoint"), 0
	}
	out := Bootstrap{
		SourceID: d.identity.SourceID,
		Epoch:    d.identity.Epoch,
		Origin:   d.identity.Origin,
		Sequence: sequence,
		Digest:   digest,
		Catalog:  d.catalog.At(sequence),
		Store:    d.store.At(sequence),
		Horizon:  d.horizon,
	}
	return out, nil, int(sequence)
}
