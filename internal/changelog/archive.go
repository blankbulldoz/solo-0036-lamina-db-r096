// Package changelog owns the retained, consumer-facing change feed.
//
// The journal is the durability log: it is replayed on startup and is only
// shortened by a checkpoint-aware rebase. The archive is a separate bounded
// resource. Its only job is to serve complete commit records to downstream
// consumers, and its history base may advance when retention policy drops
// records. A lagging consumer can therefore never force unbounded retention,
// while a write never waits for a consumer: appends are independent and
// pruning happens after a commit is already durable.
package changelog

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"lamina/internal/journal"
)

const (
	MaxFrame   = journal.MaxFrame
	headerSize = 84
)

type Archive struct {
	path       string
	base       uint64
	baseDigest string
	headDigest string
	records    []journal.Record
	sizes      []int64
	file       *os.File
	bytes      int64
	fault      error
}

func (a *Archive) Base() uint64       { return a.base }
func (a *Archive) BaseDigest() string { return a.baseDigest }
func (a *Archive) HeadDigest() string { return a.headDigest }
func (a *Archive) Bytes() int64       { return a.bytes }
func (a *Archive) Count() int         { return len(a.records) }
func (a *Archive) Fault() error       { return a.fault }

func (a *Archive) Head() uint64 {
	if len(a.records) == 0 {
		return a.base
	}
	return a.records[len(a.records)-1].Sequence
}

// Open loads or creates the archive so it covers at least the durability
// journal base. Existing records are never trusted blindly: frame CRCs, the
// per-record digest and the Prev links must all agree, otherwise startup fails.
// Records missing between the archive head and the journal base cannot be
// reconstructed and force the history base forward.
func Open(path string, journalBase uint64, journalDigest string, journalRecords []journal.Record) (*Archive, error) {
	a := &Archive{path: path, base: journalBase, baseDigest: journalDigest, headDigest: journalDigest}
	data, err := os.ReadFile(path)
	existing := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if existing {
		if err = a.parse(data); err != nil {
			return nil, err
		}
	}
	reset := !existing
	if !reset && a.Head()+1 < journalBase {
		// History between the archive head and the journal base is gone.
		reset = true
	}
	if reset {
		a.base = journalBase
		a.baseDigest = journalDigest
		a.headDigest = journalDigest
		a.records = nil
		a.sizes = nil
		if err = a.rewrite(); err != nil {
			return nil, err
		}
	}
	last := a.Head()
	digest := a.headDigest
	appended := 0
	for _, r := range journalRecords {
		if r.Sequence <= last {
			continue
		}
		if r.Digest == "" {
			return nil, fmt.Errorf("journal record %d has no lineage digest", r.Sequence)
		}
		if r.Sequence != last+1 || r.Prev != digest {
			return nil, fmt.Errorf("change archive gap at %d", r.Sequence)
		}
		if err = a.append(r, false); err != nil {
			return nil, err
		}
		last = r.Sequence
		digest = r.Digest
		appended++
	}
	if appended > 0 {
		if err = a.file.Sync(); err != nil {
			return nil, err
		}
	}
	a.headDigest = digest
	return a, nil
}

func (a *Archive) parse(data []byte) error {
	if len(data) < headerSize {
		return fmt.Errorf("change archive header truncated")
	}
	if string(data[:4]) != "LMC2" || binary.BigEndian.Uint32(data[4:8]) != 1 {
		return fmt.Errorf("invalid change archive header")
	}
	if crc32.ChecksumIEEE(data[:80]) != binary.BigEndian.Uint32(data[80:84]) {
		return fmt.Errorf("change archive header checksum mismatch")
	}
	a.base = binary.BigEndian.Uint64(data[8:16])
	a.baseDigest = string(data[16:80])
	if len(a.baseDigest) != len(journal.Genesis) || !isHex(a.baseDigest) {
		return fmt.Errorf("invalid change archive base digest")
	}
	a.bytes = headerSize
	offset := headerSize
	validEnd := headerSize
	last := a.base
	digest := a.baseDigest
	for offset < len(data) {
		if len(data)-offset < 8 {
			break
		}
		length := binary.BigEndian.Uint32(data[offset : offset+4])
		if length == 0 || length > MaxFrame {
			return fmt.Errorf("invalid change archive length at %d", offset)
		}
		if len(data)-offset-8 < int(length) {
			break
		}
		payload := data[offset+8 : offset+8+int(length)]
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(data[offset+4:offset+8]) {
			return fmt.Errorf("change archive checksum mismatch at %d", offset)
		}
		var r journal.Record
		if err := json.Unmarshal(payload, &r); err != nil {
			return fmt.Errorf("change archive json at %d: %w", offset, err)
		}
		if r.Format != 1 || r.Sequence != last+1 {
			return fmt.Errorf("change archive sequence mismatch at %d", offset)
		}
		if err := journal.Verify(r); err != nil {
			return fmt.Errorf("change archive lineage at %d: %w", offset, err)
		}
		if r.Prev != digest {
			return fmt.Errorf("change archive chain break at %d", offset)
		}
		a.records = append(a.records, r)
		a.sizes = append(a.sizes, int64(8+length))
		a.bytes += int64(8 + length)
		last = r.Sequence
		digest = r.Digest
		offset += 8 + int(length)
		validEnd = offset
	}
	if validEnd != len(data) {
		// Only an incomplete final frame is recoverable; truncate it.
		if err := os.Truncate(a.path, int64(validEnd)); err != nil {
			return err
		}
		a.bytes = int64(validEnd)
	}
	a.headDigest = digest
	f, err := os.OpenFile(a.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	a.file = f
	return nil
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

func (a *Archive) header() []byte {
	h := make([]byte, headerSize)
	copy(h[:4], "LMC2")
	binary.BigEndian.PutUint32(h[4:8], 1)
	binary.BigEndian.PutUint64(h[8:16], a.base)
	copy(h[16:80], a.baseDigest)
	binary.BigEndian.PutUint32(h[80:84], crc32.ChecksumIEEE(h[:80]))
	return h
}

// rewrite installs a new archive file atomically. It is used for creation,
// recovery from a gap, and retention pruning.
func (a *Archive) rewrite() error {
	if a.file != nil {
		a.file.Close()
		a.file = nil
	}
	dir := filepath.Dir(a.path)
	temp, err := os.CreateTemp(dir, ".lamina-changes-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err = temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	if _, err = temp.Write(a.header()); err != nil {
		temp.Close()
		return err
	}
	total := int64(headerSize)
	for _, r := range a.records {
		frame, err := encodeFrame(r)
		if err != nil {
			temp.Close()
			return err
		}
		if _, err = temp.Write(frame); err != nil {
			temp.Close()
			return err
		}
		total += int64(len(frame))
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, a.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		d.Close()
		return err
	}
	d.Close()
	f, err := os.OpenFile(a.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	a.file = f
	a.bytes = total
	return nil
}

func encodeFrame(r journal.Record) ([]byte, error) {
	r.Format = 1
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxFrame {
		return nil, fmt.Errorf("change record exceeds frame limit")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	return frame, nil
}

func (a *Archive) Append(r journal.Record) error {
	if a.fault != nil {
		return a.fault
	}
	if r.Sequence != a.Head()+1 {
		return fmt.Errorf("change archive sequence %d does not follow %d", r.Sequence, a.Head())
	}
	if r.Prev != a.headDigest {
		return fmt.Errorf("change archive lineage does not continue")
	}
	if err := a.append(r, true); err != nil {
		a.fault = err
		return err
	}
	a.headDigest = r.Digest
	return nil
}

func (a *Archive) append(r journal.Record, sync bool) error {
	frame, err := encodeFrame(r)
	if err != nil {
		return err
	}
	if _, err = a.file.Write(frame); err != nil {
		return err
	}
	if sync {
		if err = a.file.Sync(); err != nil {
			return err
		}
	}
	a.records = append(a.records, r)
	a.sizes = append(a.sizes, int64(len(frame)))
	a.bytes += int64(len(frame))
	return nil
}

// RecordsAfter returns up to limit complete records beyond after and whether
// more remain. Callers validate after against Base and Head first.
func (a *Archive) RecordsAfter(after uint64, limit int) ([]journal.Record, bool) {
	out := make([]journal.Record, 0, limit)
	more := false
	for _, r := range a.records {
		if r.Sequence <= after {
			continue
		}
		if len(out) >= limit {
			more = true
			break
		}
		out = append(out, r)
	}
	return out, more
}

// DigestAt returns the lineage digest at sequence seq, which may be the base
// itself. A consumer compares it with the digest it recorded when it applied
// that sequence, so a rebuilt source that happens to reuse the same numbers
// cannot be mistaken for the original history.
func (a *Archive) DigestAt(seq uint64) (string, bool) {
	if seq < a.base || seq > a.Head() {
		return "", false
	}
	if seq == a.base {
		return a.baseDigest, true
	}
	return a.records[seq-a.base-1].Digest, true
}

// CutForBytes returns the smallest history base whose retained frames fit the
// budget, never below the current base and never dropping below keep records.
func (a *Archive) CutForBytes(budget int64, keep int) uint64 {
	if budget <= 0 {
		return a.base
	}
	total := int64(headerSize)
	cut := a.base
	cutIndex := 0
	for i := len(a.records) - 1; i >= 0; i-- {
		if total+a.sizes[i] > budget {
			cut = a.records[i].Sequence
			cutIndex = i + 1
			break
		}
		total += a.sizes[i]
	}
	if keep > 0 && len(a.records)-cutIndex < keep {
		cutIndex = len(a.records) - keep
		if cutIndex < 0 {
			cutIndex = 0
		}
		if cutIndex == 0 {
			return a.base
		}
		cut = a.records[cutIndex-1].Sequence
	}
	return cut
}

// Prune drops records at or below cut. The new base is installed atomically so
// a crash leaves either the old or new archive, never a half-pruned feed.
func (a *Archive) Prune(cut uint64) (dropped int, freed int64, err error) {
	if cut < a.base {
		return 0, 0, fmt.Errorf("cannot move history base backwards")
	}
	if cut == a.base {
		return 0, 0, nil
	}
	if cut > a.Head() {
		return 0, 0, fmt.Errorf("cannot prune beyond archive head")
	}
	keep := 0
	var removedBytes int64
	digest := a.baseDigest
	for keep < len(a.records) && a.records[keep].Sequence <= cut {
		digest = a.records[keep].Digest
		removedBytes += a.sizes[keep]
		keep++
	}
	if keep == 0 {
		return 0, 0, nil
	}
	old := a.records
	oldSizes := a.sizes
	oldBase, oldBaseDigest := a.base, a.baseDigest
	a.records = append([]journal.Record(nil), old[keep:]...)
	a.sizes = append([]int64(nil), oldSizes[keep:]...)
	a.base = cut
	a.baseDigest = digest
	if err = a.rewrite(); err != nil {
		a.records = old
		a.sizes = oldSizes
		a.base, a.baseDigest = oldBase, oldBaseDigest
		return 0, 0, err
	}
	return keep, removedBytes, nil
}

func (a *Archive) Close() error {
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}
