// Package consumer implements an independent relational downstream of a
// LaminaDB source.
//
// The invariant it owns is the meaning of its watermark: the highest source
// commit sequence whose complete record has been applied to local state and
// durably persisted. A watermark never advances before the whole commit is
// applied, and a restart resumes exactly after it. Duplicate pages are
// harmless: applying a source commit is keyed by its source sequence, so no
// row is modified twice and no transaction is ever partially visible.
package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"lamina/internal/catalog"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/query"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
)

const (
	stateFile  = "downstream.json"
	lockFile   = "downstream.lock"
	maxState   = 256 << 20
	defaultTag = "default"
)

// Error carries a stable code for HTTP mapping.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }
func fault(code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// State is the entire durable downstream projection. Catalog, rows and the
// watermark are installed by one atomic rename, so a restart can never observe
// rows beyond the watermark or a watermark beyond the rows.
type State struct {
	Format           int              `json:"format"`
	SourceID         string           `json:"source_id"`
	Epoch            string           `json:"epoch"`
	Origin           *snapshot.Origin `json:"origin,omitempty"`
	BootstrapSeq     uint64           `json:"bootstrap_sequence"`
	Watermark        uint64           `json:"watermark"`
	Digest           string           `json:"digest"`
	Catalog          *catalog.Catalog `json:"catalog"`
	Store            *versions.Store  `json:"store"`
	AppliedRecords   uint64           `json:"applied_records"`
	AppliedWrites    uint64           `json:"applied_writes"`
	DuplicateRecords uint64           `json:"duplicate_records_skipped"`
	Initialized      bool             `json:"initialized"`
}

func emptyState() *State {
	return &State{Format: 1, Catalog: catalog.New(), Store: versions.New()}
}

func (s *State) clone() *State {
	copy := *s
	copy.Catalog = s.Catalog.Clone()
	copy.Store = s.Store.Clone()
	if s.Origin != nil {
		origin := *s.Origin
		copy.Origin = &origin
	}
	return &copy
}

type Status struct {
	Initialized    bool             `json:"initialized"`
	SourceID       string           `json:"source_id,omitempty"`
	Epoch          string           `json:"epoch,omitempty"`
	Origin         *snapshot.Origin `json:"origin,omitempty"`
	BootstrapSeq   uint64           `json:"bootstrap_sequence"`
	Watermark      uint64           `json:"watermark"`
	Digest         string           `json:"digest,omitempty"`
	Rows           int              `json:"rows"`
	Relations      int              `json:"relations"`
	AppliedRecords uint64           `json:"applied_records"`
	AppliedWrites  uint64           `json:"applied_writes"`
	Duplicates     uint64           `json:"duplicate_records_skipped"`
	SourceHead     uint64           `json:"source_head,omitempty"`
	SourceEpoch    string           `json:"source_epoch,omitempty"`
	BehindRecords  uint64           `json:"behind_records"`
	Stale          bool             `json:"stale"`
}

type SyncResult struct {
	From         uint64   `json:"from"`
	Watermark    uint64   `json:"watermark"`
	Digest       string   `json:"digest"`
	Records      int      `json:"records_applied"`
	Writes       int      `json:"writes_applied"`
	Duplicates   int      `json:"duplicate_records"`
	Pages        int      `json:"pages"`
	CaughtUp     bool     `json:"caught_up"`
	SourceHead   uint64   `json:"source_head"`
	SourceEpoch  string   `json:"source_epoch"`
	Acknowledged bool     `json:"acknowledged"`
	Warnings     []string `json:"warnings,omitempty"`
}

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

type page struct {
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
}

// Downstream is one independent consumer with its own durable directory.
type Downstream struct {
	mu     sync.Mutex
	dir    string
	source string
	name   string
	client *http.Client
	lock   *os.File
	state  *State
	fault  error
	closed bool
}

func Open(dir, source, name string) (*Downstream, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(absolute, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(absolute, lockFile), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("downstream directory already open: %w", err)
	}
	if name == "" {
		name = defaultTag
	}
	d := &Downstream{dir: absolute, source: source, name: name, client: &http.Client{Timeout: 20 * time.Second}, lock: lock}
	state, err := loadState(filepath.Join(absolute, stateFile))
	if err != nil {
		lock.Close()
		return nil, err
	}
	d.state = state
	return d, nil
}

func loadState(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return emptyState(), nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) > maxState {
		return nil, fmt.Errorf("downstream state exceeds limit")
	}
	var state State
	if err = json.Unmarshal(b, &state); err != nil {
		return nil, fmt.Errorf("downstream state: %w", err)
	}
	if state.Format != 1 || state.Catalog == nil || state.Store == nil || state.Catalog.Relations == nil || state.Store.Tables == nil {
		return nil, fmt.Errorf("invalid downstream state")
	}
	if state.Watermark < state.BootstrapSeq {
		return nil, fmt.Errorf("downstream watermark precedes bootstrap")
	}
	return &state, nil
}

// persist installs state atomically and durably. A crash either leaves the
// previous watermark or the complete new one; never a partially updated mirror.
func (d *Downstream) persist(state *State) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(b) > maxState {
		return fault("capacity", "downstream state exceeds 256MiB")
	}
	path := filepath.Join(d.dir, stateFile)
	temp, err := os.CreateTemp(d.dir, ".downstream-state-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err = temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	if _, err = temp.Write(b); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(d.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (d *Downstream) faulted() error {
	if d.closed {
		return fault("unavailable", "downstream closed")
	}
	if d.fault != nil {
		return fault("unavailable", "downstream state uncertain; restart required")
	}
	return nil
}

func (d *Downstream) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	if d.lock != nil {
		d.lock.Close()
		d.lock = nil
	}
	return nil
}

func statusOf(state *State) Status {
	keys, _ := state.Store.Count()
	return Status{
		Initialized:    state.Initialized,
		SourceID:       state.SourceID,
		Epoch:          state.Epoch,
		Origin:         state.Origin,
		BootstrapSeq:   state.BootstrapSeq,
		Watermark:      state.Watermark,
		Digest:         state.Digest,
		Rows:           keys,
		Relations:      len(state.Catalog.Relations),
		AppliedRecords: state.AppliedRecords,
		AppliedWrites:  state.AppliedWrites,
		Duplicates:     state.DuplicateRecords,
	}
}

// Status reports local facts plus a live probe of the source. The probe is
// advisory: when the source is unreachable the local watermark is unchanged
// and still authoritative.
func (d *Downstream) Status() (Status, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.faulted(); err != nil {
		return Status{}, err
	}
	out := statusOf(d.state)
	if !d.state.Initialized {
		return out, nil
	}
	head, epoch, err := d.probe()
	if err != nil {
		// The local watermark stays authoritative while the source is down.
		return out, nil
	}
	out.SourceHead = head
	out.SourceEpoch = epoch
	out.Stale = epoch != d.state.Epoch || head < d.state.Watermark
	if head >= d.state.Watermark {
		out.BehindRecords = head - d.state.Watermark
	}
	return out, nil
}

// Probe reports the source's current head and epoch without touching local
// state. It lets an operator see a rebuilt source before attempting a sync.
func (d *Downstream) Probe() (uint64, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.faulted(); err != nil {
		return 0, "", err
	}
	return d.probe()
}

func (d *Downstream) probe() (uint64, string, error) {
	response, err := d.client.Get(d.source + "/identity")
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return 0, "", fmt.Errorf("identity unavailable")
	}
	var body struct {
		Epoch    string `json:"epoch"`
		Sequence uint64 `json:"sequence"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&body); err != nil {
		return 0, "", err
	}
	return body.Sequence, body.Epoch, nil
}

// Reinitialize installs a fresh bootstrap from the source at one retained
// sequence. This is the explicit, expensive path: the whole projection is
// replaced, so the local watermark jumps to a source point that is provably
// complete instead of guessing that a reused number means the same history.
func (d *Downstream) Reinitialize(at *uint64) (Status, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.faulted(); err != nil {
		return Status{}, err
	}
	path := "/changes/bootstrap"
	if at != nil {
		path += fmt.Sprintf("?at=%d", *at)
	}
	response, err := d.client.Get(d.source + path)
	if err != nil {
		return Status{}, fault("unavailable", "bootstrap request failed: %s", err)
	}
	defer response.Body.Close()
	if response.StatusCode == 410 {
		return Status{}, fault("stale_cursor", "requested bootstrap point is no longer retained")
	}
	if response.StatusCode != 200 {
		return Status{}, fault("unavailable", "bootstrap returned %d", response.StatusCode)
	}
	var boot Bootstrap
	if err = json.NewDecoder(io.LimitReader(response.Body, maxState)).Decode(&boot); err != nil {
		return Status{}, fault("invalid", "bootstrap response: %s", err)
	}
	if boot.Catalog == nil || boot.Store == nil || boot.SourceID == "" || boot.Epoch == "" || boot.Digest == "" || boot.Catalog.Relations == nil || boot.Store.Tables == nil {
		return Status{}, fault("invalid", "incomplete bootstrap")
	}
	if boot.Horizon > boot.Sequence {
		return Status{}, fault("invalid", "bootstrap horizon beyond sequence")
	}
	state := emptyState()
	state.Initialized = true
	state.SourceID = boot.SourceID
	state.Epoch = boot.Epoch
	state.Origin = boot.Origin
	state.BootstrapSeq = boot.Sequence
	state.Watermark = boot.Sequence
	state.Digest = boot.Digest
	state.Catalog = boot.Catalog
	state.Store = boot.Store
	if err = d.persist(state); err != nil {
		d.fault = err
		return Status{}, fault("unavailable", "persist bootstrap: %s", err)
	}
	d.state = state
	return statusOf(state), nil
}

// SyncOptions bounds one sync call. ReplayFrom re-reads already applied
// commits to verify the source still serves the same lineage; it never moves
// the watermark or local rows.
type SyncOptions struct {
	MaxPages   int
	PageLimit  int
	ReplayFrom *uint64
}

func (d *Downstream) Sync(options SyncOptions) (SyncResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.faulted(); err != nil {
		return SyncResult{}, err
	}
	if !d.state.Initialized {
		return SyncResult{}, fault("not_initialized", "downstream has no bootstrap; call reinitialize")
	}
	if options.MaxPages <= 0 {
		options.MaxPages = 100
	}
	if options.PageLimit <= 0 || options.PageLimit > 1000 {
		options.PageLimit = 200
	}
	from := d.state.Watermark
	replaying := options.ReplayFrom != nil
	if replaying {
		if *options.ReplayFrom < d.state.BootstrapSeq {
			return SyncResult{}, fault("invalid", "replay point precedes bootstrap")
		}
		if *options.ReplayFrom > d.state.Watermark {
			return SyncResult{}, fault("invalid", "replay point is ahead of the watermark")
		}
		from = *options.ReplayFrom
	}
	// Check identity before reading any page. A rebuilt source may have a head
	// below the local watermark, in which case the page request itself would
	// fail; the epoch tells us the real reason.
	head, epoch, err := d.probe()
	if err != nil {
		return SyncResult{}, fault("unavailable", "source unavailable: %s", err)
	}
	if epoch != d.state.Epoch {
		return SyncResult{}, fault("identity_changed", "source identity changed: local %s/%s, source %s/%s; reinitialize from a bootstrap",
			d.state.SourceID, d.state.Epoch, d.state.SourceID, epoch)
	}
	if head < d.state.Watermark {
		return SyncResult{}, fault("discontinuity", "source head %d is behind the applied watermark %d", head, d.state.Watermark)
	}
	result := SyncResult{From: from, SourceEpoch: d.state.Epoch, SourceHead: head}
	after := from
	expectedPrev := ""
	appliedBefore := d.state.AppliedWrites
	duplicates := 0
	for pageCount := 0; pageCount < options.MaxPages; pageCount++ {
		if after == d.state.Watermark {
			// Resuming at the watermark: the next record must continue the exact
			// lineage the local state recorded. This is the check that rejects a
			// source rebuilt from a backup whose numbers happen to coincide.
			expectedPrev = d.state.Digest
		}
		body, err := d.fetchPage(after, options.PageLimit)
		if err != nil {
			return result, err
		}
		result.Pages++
		if body.Head > result.SourceHead {
			result.SourceHead = body.Head
		}
		if body.SourceID != d.state.SourceID || body.Epoch != d.state.Epoch {
			return result, fault("identity_changed", "source identity changed: local %s/%s, source %s/%s; reinitialize from a bootstrap",
				d.state.SourceID, d.state.Epoch, body.SourceID, body.Epoch)
		}
		if body.HistoryBase > d.state.Watermark {
			return result, fault("stale_cursor", "source history begins at %d, beyond watermark %d; reinitialize", body.HistoryBase, d.state.Watermark)
		}
		if len(body.Records) == 0 {
			if body.Cursor != after {
				return result, fault("discontinuity", "source returned cursor %d for request %d", body.Cursor, after)
			}
			// An empty page repeats the digest at the requested cursor, so a
			// source that was rebuilt under the same number is caught here even
			// when it has no new commits to deliver.
			if after == d.state.Watermark && body.CursorDigest != "" && body.CursorDigest != d.state.Digest {
				return result, fault("discontinuity", "source lineage at %d does not match the applied digest", after)
			}
			result.CaughtUp = !body.More
			break
		}
		if body.Records[0].Sequence != after+1 {
			return result, fault("discontinuity", "source page begins at %d, expected %d", body.Records[0].Sequence, after+1)
		}
		for _, record := range body.Records {
			if expectedPrev != "" && record.Prev != expectedPrev {
				return result, fault("discontinuity", "record %d predecessor does not follow the applied chain", record.Sequence)
			}
			if record.Sequence <= d.state.Watermark {
				if err = journal.Verify(record); err != nil {
					return result, fault("discontinuity", "replayed source record %d: %s", record.Sequence, err)
				}
				if err = d.recordDuplicate(); err != nil {
					return result, err
				}
				duplicates++
				expectedPrev = record.Digest
				continue
			}
			if err = d.applyCommit(record); err != nil {
				return result, err
			}
			result.Records++
			expectedPrev = record.Digest
		}
		after = body.Records[len(body.Records)-1].Sequence
		if !body.More {
			result.CaughtUp = true
			break
		}
		if pageCount == options.MaxPages-1 {
			// The page budget ran out with source records still pending. The
			// watermark is durable at the last fully applied commit, but this
			// call did not reach the source head.
			break
		}
	}
	if expectedPrev != "" && after == d.state.Watermark && expectedPrev != d.state.Digest {
		return result, fault("discontinuity", "replayed lineage at %d does not match the applied digest", after)
	}
	result.Watermark = d.state.Watermark
	result.Digest = d.state.Digest
	result.Writes = int(d.state.AppliedWrites - appliedBefore)
	result.Duplicates = duplicates
	if result.CaughtUp {
		ok, warning := d.acknowledge()
		result.Acknowledged = ok
		if warning != "" {
			result.Warnings = append(result.Warnings, warning)
		}
	}
	return result, nil
}

func (d *Downstream) fetchPage(after uint64, limit int) (*page, error) {
	response, err := d.client.Get(fmt.Sprintf("%s/changes?after=%d&limit=%d", d.source, after, limit))
	if err != nil {
		return nil, fault("unavailable", "source unavailable: %s", err)
	}
	defer response.Body.Close()
	if response.StatusCode == 410 {
		return nil, fault("stale_cursor", "source no longer retains cursor %d; reinitialize", after)
	}
	if response.StatusCode == 400 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fault("discontinuity", "source rejected cursor %d: %s", after, bytes.TrimSpace(data))
	}
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fault("unavailable", "source returned %d: %s", response.StatusCode, bytes.TrimSpace(data))
	}
	var body page
	if err = json.NewDecoder(io.LimitReader(response.Body, maxState)).Decode(&body); err != nil {
		return nil, fault("invalid", "source page: %s", err)
	}
	return &body, nil
}

// applyCommit applies one complete source commit and persists the new
// watermark. Mutations whose key already has a version at this sequence or
// later are skipped, so redelivering a commit is a no-op for data. Every write
// in the record is applied before the watermark is persisted, so no observer
// can see a transaction split across the watermark.
func (d *Downstream) applyCommit(record journal.Record) error {
	if err := journal.Verify(record); err != nil {
		return fault("discontinuity", "source record %d: %s", record.Sequence, err)
	}
	if record.Sequence != d.state.Watermark+1 {
		return fault("discontinuity", "record %d skips from watermark %d", record.Sequence, d.state.Watermark)
	}
	if record.Prev != d.state.Digest {
		return fault("discontinuity", "record %d predecessor does not match applied digest", record.Sequence)
	}
	next := d.state.clone()
	applied := applyTo(next, record)
	next.Watermark = record.Sequence
	next.Digest = record.Digest
	next.AppliedRecords++
	next.AppliedWrites += uint64(applied)
	if err := d.persist(next); err != nil {
		d.fault = err
		return fault("unavailable", "persist commit %d: %s", record.Sequence, err)
	}
	d.state = next
	return nil
}

// recordDuplicate persists the observational fact that a page was redelivered.
// It never touches rows, the watermark or the lineage digest.
func (d *Downstream) recordDuplicate() error {
	next := d.state.clone()
	next.DuplicateRecords++
	if err := d.persist(next); err != nil {
		d.fault = err
		return fault("unavailable", "persist duplicate counter: %s", err)
	}
	d.state = next
	return nil
}

// applyTo mutates the cloned state by one commit. It is idempotent because a
// key that already carries a version at or beyond this commit's sequence is
// skipped, which is what makes duplicate pages safe.
func applyTo(state *State, record journal.Record) int {
	switch record.Kind {
	case "relation":
		if record.Relation == nil || record.Relation.Created != record.Sequence {
			return 0
		}
		if _, exists := state.Catalog.Relations[record.Relation.Name]; exists {
			return 0
		}
		state.Catalog.Relations[record.Relation.Name] = *record.Relation
		state.Store.Ensure(record.Relation.Name)
		return 0
	case "index":
		if record.Index == nil || record.Index.Created != record.Sequence {
			return 0
		}
		if _, exists := state.Catalog.Indexes[record.Index.Name]; exists {
			return 0
		}
		state.Catalog.Indexes[record.Index.Name] = *record.Index
		return 0
	case "commit":
		applied := 0
		for _, mutation := range record.Writes {
			state.Store.Ensure(mutation.Relation)
			if state.Store.Latest(mutation.Relation, mutation.Key) >= record.Sequence {
				continue
			}
			state.Store.Apply(record.Sequence, []versions.Mutation{mutation})
			applied++
		}
		return applied
	default:
		return 0
	}
}

func (d *Downstream) acknowledge() (bool, string) {
	body, err := json.Marshal(map[string]any{"consumer": d.name, "watermark": d.state.Watermark, "digest": d.state.Digest})
	if err != nil {
		return false, ""
	}
	response, err := d.client.Post(d.source+"/changes/ack", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, "source acknowledgement failed: " + err.Error()
	}
	defer response.Body.Close()
	if response.StatusCode == 200 {
		return true, ""
	}
	if response.StatusCode == 410 {
		return false, "source no longer retains the acknowledged watermark; retention overtook consumer"
	}
	return false, fmt.Sprintf("source acknowledgement returned %d", response.StatusCode)
}

// Query evaluates a structured query over the local mirror at the watermark or
// at an explicitly retained local sequence. Because the mirror is replaced only
// after a complete commit, a caller can never observe half of a transaction.
func (d *Downstream) Query(request query.Request) (query.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.faulted(); err != nil {
		return query.Result{}, err
	}
	if !d.state.Initialized {
		return query.Result{}, fault("not_initialized", "downstream has no bootstrap")
	}
	if request.Transaction != "" {
		return query.Result{}, fault("invalid", "downstream does not provide source transactions")
	}
	relation, err := d.state.Catalog.Relation(request.Relation)
	if err != nil {
		return query.Result{}, fault("invalid", "%s", err)
	}
	at := d.state.Watermark
	if request.At != nil {
		at = *request.At
		if at < d.state.BootstrapSeq || at > d.state.Watermark {
			return query.Result{}, fault("snapshot", "available downstream interval %d..%d", d.state.BootstrapSeq, d.state.Watermark)
		}
		if relation.Created > at {
			return query.Result{}, fault("snapshot", "relation did not exist at downstream snapshot")
		}
	}
	if err = query.Bind(relation, request); err != nil {
		return query.Result{}, fault("invalid", "%s", err)
	}
	rows := d.state.Store.Scan(request.Relation, at)
	registry := index.Build(d.state.Catalog, d.state.Store)
	plan, keys := query.Choose(d.state.Catalog, registry, request, at, len(rows))
	result, err := query.Execute(relation, request, rows, plan, keys)
	if err != nil {
		return query.Result{}, fault("invalid", "%s", err)
	}
	return result, nil
}
