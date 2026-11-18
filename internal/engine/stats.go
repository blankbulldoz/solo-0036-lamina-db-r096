package engine

import "sort"

type Stats struct {
	Sequence       uint64         `json:"sequence"`
	Horizon        uint64         `json:"horizon"`
	Active         []Transaction  `json:"active"`
	Keys           int            `json:"keys"`
	Versions       int            `json:"versions"`
	IndexEntries   map[string]int `json:"index_entries"`
	JournalRecords int            `json:"journal_records"`
	HistoryBase    uint64         `json:"history_base"`
	RecoveredTail  int64          `json:"recovered_tail_bytes"`
	Commits        uint64         `json:"commits_since_open"`
	Conflicts      uint64         `json:"conflicts_since_open"`
	Queries        uint64         `json:"queries_since_open"`
	SourceID       string         `json:"source_id"`
	Epoch          string         `json:"epoch"`
	JournalBase    uint64         `json:"journal_base"`
	ChangeBase     uint64         `json:"change_base"`
	ChangeHead     uint64         `json:"change_head"`
	ChangeBytes    int64          `json:"change_bytes"`
	Consumers      int            `json:"consumers"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries, SourceID: d.identity.SourceID, Epoch: d.identity.Epoch, JournalBase: d.wal.Base, ChangeBase: d.archive.Base(), ChangeHead: d.archive.Head(), ChangeBytes: d.archive.Bytes(), Consumers: len(d.consumers)}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	return s, nil
}
