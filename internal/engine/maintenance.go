package engine

import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"time"
)

// image captures the live state along with the lineage needed to keep the
// checkpoints, journal and change archive verifiably connected.
func (d *DB) image() snapshot.Image {
	image := snapshot.Image{
		Format:   1,
		Sequence: d.sequence,
		Horizon:  d.horizon,
		SourceID: d.identity.SourceID,
		Epoch:    d.identity.Epoch,
		Origin:   d.identity.Origin,
		Catalog:  d.catalog.Clone(),
		Store:    d.store.Clone(),
	}
	if digest, ok := d.wal.DigestAt(d.sequence); ok {
		image.Digest = digest
	}
	if d.retention.MaxBytes > 0 || d.retention.MaxLagRecords > 0 || d.retention.MaxLagSeconds > 0 {
		image.Retention = &snapshot.Retention{MaxBytes: d.retention.MaxBytes, MaxLagRecords: d.retention.MaxLagRecords, MaxLagSeconds: d.retention.MaxLagSeconds}
	}
	return image
}

func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	image := d.image()
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), image); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	// The checkpoint now covers everything at or below the sequence. Rebase the
	// durability journal so its size is bounded by the interval since the last
	// checkpoint instead of growing forever. The change archive is independent
	// and keeps serving consumers beyond this base.
	if err := d.wal.Rebase(d.sequence); err != nil {
		return 0, fault("unavailable", "journal rebase: %s", err)
	}
	d.pruneLocked(time.Now())
	return d.sequence, nil
}

func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}

func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "changes.lmc"))
			os.Remove(filepath.Join(dir, snapshot.IdentityFile))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	// A restored database keeps the logical source identity but gets a new
	// epoch. Consumers that recorded the old epoch will refuse to continue and
	// must reinitialize, which is the only correct outcome: this history starts
	// from a different point even if its sequence numbers overlap.
	identity, err := snapshot.InheritIdentity(image.SourceID, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.WriteIdentity(dir, identity); err != nil {
		return err
	}
	baseDigest := journal.Genesis
	if image.Sequence > 0 && image.Digest != "" {
		baseDigest = image.Digest
	}
	w, err := journal.Open(dir, image.Sequence, baseDigest)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}

func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	beforeKeys, beforeVersions := d.store.Count()
	_ = beforeKeys
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return versions.VacuumResult{}, err
	}
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	return versions.VacuumResult{Horizon: horizon, Removed: beforeVersions - remaining, Retained: remaining, Pinned: horizon < requested}, nil
}
