package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/changelog"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type DB struct {
	mu           sync.RWMutex
	dir          string
	identity     snapshot.Identity
	catalog      *catalog.Catalog
	store        *versions.Store
	indexes      *index.Registry
	wal          *journal.WAL
	archive      *changelog.Archive
	retention    RetentionPolicy
	consumers    map[string]Consumption
	sequence     uint64
	horizon      uint64
	transactions map[string]*Transaction
	closed       bool
	commits      uint64
	conflicts    uint64
	queries      uint64
}

func Open(dir string) (*DB, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(absolute, 0700); err != nil {
		return nil, err
	}
	image, err := snapshot.Read(filepath.Join(absolute, "checkpoint.lmi"))
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	if !missing {
		if _, err = os.Stat(filepath.Join(absolute, "journal.lmw")); err != nil {
			return nil, fmt.Errorf("checkpoint without journal; use validated restore")
		}
	}
	var inherited *snapshot.Identity
	if !missing && image.SourceID != "" && image.Epoch != "" {
		value := snapshot.Identity{Format: 1, SourceID: image.SourceID, Epoch: image.Epoch, Origin: image.Origin}
		inherited = &value
	}
	identity, _, err := snapshot.LoadIdentity(absolute, inherited)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	baseDigest := journal.Genesis
	if missing || image.Sequence == 0 {
		baseDigest = journal.Genesis
	} else if image.Digest != "" {
		baseDigest = image.Digest
	}
	w, err := journal.Open(absolute, image.Sequence, baseDigest)
	if err != nil {
		return nil, err
	}
	d := &DB{dir: absolute, identity: identity, catalog: catalog.New(), store: versions.New(), wal: w, transactions: map[string]*Transaction{}, consumers: map[string]Consumption{}, retention: RetentionPolicy{MaxBytes: defaultMaxBytes, MaxLagRecords: defaultMaxLagRecords}}
	if !missing {
		d.catalog = image.Catalog
		d.store = image.Store
		d.sequence = image.Sequence
		d.horizon = image.Horizon
		if image.Retention != nil {
			d.retention = RetentionPolicy{MaxBytes: image.Retention.MaxBytes, MaxLagRecords: image.Retention.MaxLagRecords, MaxLagSeconds: image.Retention.MaxLagSeconds}
		}
	}
	// The checkpoint and the durability journal must describe the same
	// history. A digest carried in the image pins the exact sequence; if the
	// replayed chain disagrees, the directory is a spliced pair and must not
	// open, even though every frame CRC is valid.
	if !missing && image.Digest != "" {
		if digest, ok := w.DigestAt(d.sequence); !ok || digest != image.Digest {
			w.Close()
			return nil, fmt.Errorf("checkpoint lineage does not match journal history at %d", d.sequence)
		}
	}
	last := w.Base + uint64(len(w.Records))
	if d.sequence > last {
		w.Close()
		return nil, fmt.Errorf("checkpoint ahead of journal")
	}
	for _, r := range w.Records {
		if r.Sequence <= d.sequence {
			continue
		}
		if err = d.apply(r); err != nil {
			w.Close()
			return nil, fmt.Errorf("replay sequence %d: %w", r.Sequence, err)
		}
	}
	if err = d.loadRetention(); err != nil {
		w.Close()
		return nil, err
	}
	if err = d.loadConsumers(); err != nil {
		w.Close()
		return nil, err
	}
	if err = d.openArchive(); err != nil {
		w.Close()
		return nil, err
	}
	d.indexes = index.Build(d.catalog, d.store)
	for _, relation := range d.catalog.List() {
		if err = d.unique(relation.Name, nil); err != nil {
			w.Close()
			return nil, fmt.Errorf("recovered constraints: %w", err)
		}
	}
	return d, nil
}
func (d *DB) usable() error {
	if d.closed {
		return fault("unavailable", "database closed")
	}
	if d.wal.Poisoned {
		return fault("unavailable", "journal write outcome uncertain; restart required")
	}
	if d.archive.Fault() != nil {
		return fault("unavailable", "change archive unavailable; restart required")
	}
	return nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.transactions = map[string]*Transaction{}
	if d.archive != nil {
		d.archive.Close()
	}
	return d.wal.Close()
}

// publish makes one record durable and visible. Ordering is: stamp and append
// to the journal, fsync, apply, then retain the same stamped bytes in the
// change archive. A consumer can only see a commit that the source itself has
// applied; the archive is never ahead of the database.
func (d *DB) publish(r journal.Record) error {
	r.Sequence = d.sequence + 1
	r.Format = 1
	stamped, err := d.wal.Append(r)
	if err != nil {
		return fault("unavailable", "durable commit failed: %s", err)
	}
	if err = d.apply(stamped); err != nil {
		d.wal.Poisoned = true
		return fault("unavailable", "durable record cannot be applied: %s", err)
	}
	if len(stamped.Writes) > 0 {
		d.indexes.Apply(d.catalog, stamped.Writes)
	}
	if err = d.archive.Append(stamped); err != nil {
		d.wal.Poisoned = true
		return fault("unavailable", "change archive append failed: %s", err)
	}
	if d.retention.MaxBytes > 0 && d.archive.Bytes() > d.retention.MaxBytes {
		d.pruneLocked(time.Now().UTC())
	}
	return nil
}
func (d *DB) Relations() ([]catalog.Relation, []catalog.Index, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, nil, err
	}
	return d.catalog.List(), d.catalog.IndexList(), nil
}
