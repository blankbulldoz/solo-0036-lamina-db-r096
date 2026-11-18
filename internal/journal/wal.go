package journal

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	MaxFrame     = 16 << 20
	legacyHeader = 16
	headerSize   = 48
)

// WAL is the durability log. Every record carries the digest of its
// predecessor, so replay can reject a file whose history was spliced even if
// every individual CRC is intact. Records are only shortened by Rebase, which
// installs a new header naming the digest at the new base; the chain therefore
// remains verifiable across checkpoints without retaining the whole history.
type WAL struct {
	Base          uint64
	dir           string
	file          *os.File
	lock          *os.File
	Records       []Record
	RecoveredTail int64
	Poisoned      bool
	baseDigest    string
	headDigest    string
	digests       []string
	legacy        bool
}

// Open opens the directory journal for base. baseDigest is the lineage digest
// the caller believes sits at base (empty when unknown). A new journal adopts
// the supplied digest so a restored database keeps one verifiable chain; an
// existing journal must not belong to a different history.
func Open(dir string, base uint64, baseDigest string) (*WAL, error) {
	if baseDigest == "" {
		baseDigest = Genesis
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lamina.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "journal.lmw"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	w := &WAL{file: f, lock: lock, Base: base, dir: dir, baseDigest: baseDigest, headDigest: baseDigest}
	info, err := f.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err = w.writeHeader(); err != nil {
			w.Close()
			return nil, err
		}
	}
	if _, err = f.Seek(0, 0); err != nil {
		w.Close()
		return nil, err
	}
	if err = w.recover(); err != nil {
		w.Close()
		return nil, err
	}
	if w.legacy {
		if err = w.Rebase(w.Base); err != nil {
			w.Close()
			return nil, fmt.Errorf("journal format upgrade: %w", err)
		}
	}
	return w, nil
}

func (w *WAL) writeHeader() error {
	header := make([]byte, headerSize)
	copy(header[:4], "LMW2")
	binary.BigEndian.PutUint64(header[4:12], w.Base)
	digest, err := hex.DecodeString(w.baseDigest)
	if err != nil || len(digest) != 32 {
		return fmt.Errorf("invalid base digest")
	}
	copy(header[12:44], digest)
	binary.BigEndian.PutUint32(header[44:48], crc32.ChecksumIEEE(header[:44]))
	if _, err = w.file.Write(header); err != nil {
		return err
	}
	if err = w.file.Sync(); err != nil {
		return err
	}
	return w.syncDir()
}

func (w *WAL) syncDir() error {
	d, err := os.Open(w.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (w *WAL) recover() error {
	var magic [4]byte
	if _, err := io.ReadFull(w.file, magic[:]); err != nil {
		return fmt.Errorf("journal header: %w", err)
	}
	switch string(magic[:]) {
	case "LMW2":
		var rest [headerSize - 4]byte
		if _, err := io.ReadFull(w.file, rest[:]); err != nil {
			return fmt.Errorf("journal header: %w", err)
		}
		if crc32.ChecksumIEEE(append(append([]byte(nil), magic[:]...), rest[:40]...)) != binary.BigEndian.Uint32(rest[40:44]) {
			return fmt.Errorf("invalid journal header")
		}
		actualBase := binary.BigEndian.Uint64(rest[0:8])
		if actualBase > w.Base {
			return fmt.Errorf("journal requires newer checkpoint")
		}
		headerDigest := hex.EncodeToString(rest[8:40])
		if actualBase == w.Base && w.baseDigest != Genesis && headerDigest != w.baseDigest {
			return fmt.Errorf("journal belongs to a different history")
		}
		w.baseDigest = headerDigest
		if w.Base == 0 && w.baseDigest != Genesis {
			return fmt.Errorf("journal base digest is not a genesis")
		}
	case "LMW1":
		var rest [legacyHeader - 4]byte
		if _, err := io.ReadFull(w.file, rest[:]); err != nil {
			return fmt.Errorf("journal header: %w", err)
		}
		if crc32.ChecksumIEEE(append([]byte("LMW1"), rest[:8]...)) != binary.BigEndian.Uint32(rest[8:12]) {
			return fmt.Errorf("invalid journal header")
		}
		actualBase := binary.BigEndian.Uint64(rest[0:8])
		if actualBase > w.Base {
			return fmt.Errorf("journal requires newer checkpoint")
		}
		w.Base = actualBase
		w.baseDigest = Genesis
		w.legacy = true
	default:
		return fmt.Errorf("invalid journal header")
	}
	w.headDigest = w.baseDigest
	w.digests = nil
	offset := int64(headerSize)
	if w.legacy {
		offset = legacyHeader
	}
	last := w.Base
	for {
		var head [8]byte
		n, err := io.ReadFull(w.file, head[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			w.RecoveredTail = int64(n)
			break
		}
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 || length > MaxFrame {
			return fmt.Errorf("invalid journal length at %d", offset)
		}
		payload := make([]byte, length)
		n, err = io.ReadFull(w.file, payload)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			w.RecoveredTail = int64(8 + n)
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[4:]) {
			return fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		var r Record
		if err = json.Unmarshal(payload, &r); err != nil {
			return fmt.Errorf("journal json at %d: %w", offset, err)
		}
		if r.Format != 1 || r.Sequence != last+1 {
			return fmt.Errorf("journal sequence or format mismatch at %d", offset)
		}
		if r.Digest == "" {
			// Pre-lineage record: stamp it deterministically so the chain is
			// reproducible, and rewrite the journal below.
			w.legacy = true
			r.Prev = w.headDigest
			if r, err = Stamp(r); err != nil {
				return err
			}
		} else {
			if r.Prev != w.headDigest {
				return fmt.Errorf("journal lineage break at %d", offset)
			}
			if err = Verify(r); err != nil {
				return fmt.Errorf("journal lineage at %d: %w", offset, err)
			}
		}
		w.Records = append(w.Records, r)
		w.digests = append(w.digests, r.Digest)
		w.headDigest = r.Digest
		last = r.Sequence
		offset += int64(8 + length)
	}
	if w.RecoveredTail > 0 {
		if err := w.file.Truncate(offset); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

// Append stamps and durably writes the next record. It returns the stamped
// record so the change archive can retain exactly the bytes the durability log
// will replay.
func (w *WAL) Append(r Record) (Record, error) {
	if w.Poisoned {
		return r, fmt.Errorf("journal is unavailable; restart required")
	}
	if r.Sequence != w.Base+uint64(len(w.Records))+1 {
		return r, fmt.Errorf("noncontiguous journal sequence")
	}
	r.Format = 1
	r.Prev = w.headDigest
	r.Digest = ""
	stamped, err := Stamp(r)
	if err != nil {
		return r, err
	}
	payload, err := json.Marshal(stamped)
	if err != nil {
		return r, err
	}
	if len(payload) > MaxFrame {
		return r, fmt.Errorf("commit exceeds journal frame limit")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if _, err = w.file.Write(frame); err != nil {
		w.Poisoned = true
		return r, err
	}
	if err = w.file.Sync(); err != nil {
		w.Poisoned = true
		return r, err
	}
	w.Records = append(w.Records, stamped)
	w.digests = append(w.digests, stamped.Digest)
	w.headDigest = stamped.Digest
	return stamped, nil
}

func (w *WAL) HeadDigest() string { return w.headDigest }
func (w *WAL) BaseDigest() string { return w.baseDigest }
func (w *WAL) Head() uint64       { return w.Base + uint64(len(w.Records)) }
func (w *WAL) DigestAt(seq uint64) (string, bool) {
	if seq < w.Base {
		return "", false
	}
	if seq == w.Base {
		return w.baseDigest, true
	}
	index := int(seq - w.Base - 1)
	if index >= len(w.digests) {
		return "", false
	}
	return w.digests[index], true
}

// Rebase atomically installs a new journal file beginning at base. Records at
// or below base are dropped; the digest at base is carried in the header so the
// chain stays verifiable. base must be at or beyond the current base and no
// further than the current head.
func (w *WAL) Rebase(base uint64) error {
	if base < w.Base {
		return fmt.Errorf("journal base cannot move backwards")
	}
	if base > w.Head() {
		return fmt.Errorf("journal base beyond head")
	}
	digest, ok := w.DigestAt(base)
	if !ok {
		return fmt.Errorf("journal digest at %d unavailable", base)
	}
	keep := 0
	for keep < len(w.Records) && w.Records[keep].Sequence <= base {
		keep++
	}
	records := append([]Record(nil), w.Records[keep:]...)
	digests := append([]string(nil), w.digests[keep:]...)
	temp, err := os.CreateTemp(w.dir, ".lamina-journal-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	header := make([]byte, headerSize)
	copy(header[:4], "LMW2")
	binary.BigEndian.PutUint64(header[4:12], base)
	raw, err := hex.DecodeString(digest)
	if err != nil || len(raw) != 32 {
		temp.Close()
		return fmt.Errorf("invalid rebase digest")
	}
	copy(header[12:44], raw)
	binary.BigEndian.PutUint32(header[44:48], crc32.ChecksumIEEE(header[:44]))
	if _, err = temp.Write(header); err != nil {
		temp.Close()
		return err
	}
	for _, r := range records {
		payload, err := json.Marshal(r)
		if err != nil {
			temp.Close()
			return err
		}
		frame := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
		copy(frame[8:], payload)
		if _, err = temp.Write(frame); err != nil {
			temp.Close()
			return err
		}
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(w.dir, "journal.lmw")); err != nil {
		return err
	}
	if err = w.syncDir(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(w.dir, "journal.lmw"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		w.Poisoned = true
		return err
	}
	old := w.file
	w.file = f
	if old != nil {
		old.Close()
	}
	w.Base = base
	w.baseDigest = digest
	w.headDigest = digest
	w.Records = records
	w.digests = digests
	for _, r := range records {
		w.headDigest = r.Digest
	}
	w.legacy = false
	return nil
}

func (w *WAL) Close() error {
	var err error
	if w.file != nil {
		err = w.file.Close()
		w.file = nil
	}
	if w.lock != nil {
		w.lock.Close()
		w.lock = nil
	}
	return err
}
