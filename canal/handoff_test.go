package canal

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/dump"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestStore(t *testing.T) *FileHandoffStore {
	t.Helper()
	dir := t.TempDir()
	// A tiny TTL lets a "killed" owner's lock be reclaimed by the next run.
	s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func reopenTestStore(t *testing.T, dir string) *FileHandoffStore {
	t.Helper()
	s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func validStart() handoffStartPoint {
	return handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 154}
}

func newTestManifest(batchSize int, start handoffStartPoint, captured bool, tables ...string) *snapshotManifest {
	m := &snapshotManifest{
		Version:       handoffVersion,
		Flavor:        "mysql",
		BatchSize:     batchSize,
		Start:         start,
		StartCaptured: captured,
		Tables:        make(map[string]*snapshotTableProgress, len(tables)),
		CreatedAt:     time.Unix(1700000000, 0),
		UpdatedAt:     time.Unix(1700000000, 0),
	}
	for _, key := range tables {
		m.Tables[key] = &snapshotTableProgress{State: snapshotStatePending}
	}
	return m
}

// ---------------------------------------------------------------------------
// File store
// ---------------------------------------------------------------------------

func TestFileHandoffStore_SnapshotRoundtrip(t *testing.T) {
	s := newTestStore(t)

	m, exists, err := s.loadSnapshot()
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, m)

	want := newTestManifest(10, validStart(), true, "db.a", "db.b")
	want.Tables["db.a"].CompletedBatches = 3
	require.NoError(t, s.SaveSnapshot(want))

	got, exists, err := s.loadSnapshot()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, want, got)
}

func TestFileHandoffStore_CommitAndLoadHandoff(t *testing.T) {
	s := newTestStore(t)

	m := newTestManifest(10, validStart(), true, "db.a", "db.b")
	m.Tables["db.a"].State = snapshotStateDone
	m.Tables["db.a"].CompletedBatches = 2
	m.Tables["db.b"].State = snapshotStateDone
	m.Tables["db.b"].CompletedBatches = 1
	require.NoError(t, s.SaveSnapshot(m))

	r := &handoffRecord{
		Version:          handoffVersion,
		Flavor:           "mysql",
		Start:            validStart(),
		SnapshotComplete: true,
		Tables:           2,
		CompletedBatches: 3,
		CompletedAt:      time.Now(),
	}
	require.NoError(t, s.CommitHandoff(r))

	got, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, r.Start, got.Start)
	require.True(t, got.SnapshotComplete)
}

func TestFileHandoffStore_TmpFilesAreNotCommitted(t *testing.T) {
	s := newTestStore(t)

	// A leftover temp file from a crash during write must not look committed.
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), handoffFileName+".tmp"),
		[]byte("{garbage"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), snapshotFileName+".tmp"),
		[]byte("{garbage"), 0o600))

	r, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, r)

	m, exists, err := s.loadSnapshot()
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, m)
}

func TestFileHandoffStore_CorruptHandoffFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		record any
		// mutate allows breaking manifest/handoff consistency.
		mutate func(t *testing.T, dir string)
	}{
		{
			name:   "garbage json",
			mutate: func(t *testing.T, dir string) { requireWrite(t, filepath.Join(dir, handoffFileName), "{not json") },
		},
		{
			name:   "snapshot not complete",
			record: handoffRecord{Version: handoffVersion, Start: validStart(), Tables: 1, CompletedBatches: 1, CompletedAt: time.Now()},
		},
		{
			name:   "zero start point",
			record: handoffRecord{Version: handoffVersion, SnapshotComplete: true, Tables: 1, CompletedBatches: 1, CompletedAt: time.Now()},
		},
		{
			name:   "zero timestamp",
			record: handoffRecord{Version: handoffVersion, SnapshotComplete: true, Start: validStart(), Tables: 1, CompletedBatches: 1},
		},
		{
			name:   "unsupported version",
			record: handoffRecord{Version: 99, SnapshotComplete: true, Start: validStart(), Tables: 1, CompletedBatches: 1, CompletedAt: time.Now()},
		},
		{
			name:   "missing manifest",
			record: handoffRecord{Version: handoffVersion, SnapshotComplete: true, Start: validStart(), Tables: 1, CompletedBatches: 1, CompletedAt: time.Now()},
			mutate: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(filepath.Join(dir, snapshotFileName)))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			dir := s.Dir()

			m := newTestManifest(10, validStart(), true, "db.a")
			m.Tables["db.a"].State = snapshotStateDone
			m.Tables["db.a"].CompletedBatches = 1
			require.NoError(t, s.SaveSnapshot(m))

			if tc.mutate != nil && tc.record == nil {
				tc.mutate(t, dir)
			} else if tc.record != nil {
				requireWriteJSON(t, filepath.Join(dir, handoffFileName), tc.record)
				if tc.mutate != nil {
					tc.mutate(t, dir)
				}
			}

			got, exists, err := s.loadHandoff()
			require.Error(t, err)
			require.True(t, errors.Cause(err) == ErrIncompleteHandoff, "expected ErrIncompleteHandoff, got %v", err)
			require.False(t, exists)
			require.Nil(t, got)
		})
	}
}

func TestFileHandoffStore_HandoffManifestMismatchFailsClosed(t *testing.T) {
	s := newTestStore(t)
	dir := s.Dir()

	m := newTestManifest(10, validStart(), true, "db.a")
	m.Tables["db.a"].State = snapshotStateDone
	m.Tables["db.a"].CompletedBatches = 1
	require.NoError(t, s.SaveSnapshot(m))

	// record claims a different start point
	other := validStart()
	other.BinlogPos = 4096
	requireWriteJSON(t, filepath.Join(dir, handoffFileName), handoffRecord{
		Version: handoffVersion, SnapshotComplete: true, Start: other,
		Tables: 1, CompletedBatches: 1, CompletedAt: time.Now(),
	})
	_, _, err := s.loadHandoff()
	require.ErrorIs(t, errors.Cause(err), ErrIncompleteHandoff)

	// record counts disagree with the manifest
	other = validStart()
	requireWriteJSON(t, filepath.Join(dir, handoffFileName), handoffRecord{
		Version: handoffVersion, SnapshotComplete: true, Start: other,
		Tables: 2, CompletedBatches: 5, CompletedAt: time.Now(),
	})
	_, _, err = s.loadHandoff()
	require.ErrorIs(t, errors.Cause(err), ErrIncompleteHandoff)

	// manifest still has an unfinished table
	m.Tables["db.a"].State = snapshotStateDumping
	require.NoError(t, s.SaveSnapshot(m))
	requireWriteJSON(t, filepath.Join(dir, handoffFileName), handoffRecord{
		Version: handoffVersion, SnapshotComplete: true, Start: validStart(),
		Tables: 1, CompletedBatches: 1, CompletedAt: time.Now(),
	})
	_, _, err = s.loadHandoff()
	require.ErrorIs(t, errors.Cause(err), ErrIncompleteHandoff)
}

func TestFileHandoffStore_CorruptManifestFailsClosed(t *testing.T) {
	s := newTestStore(t)
	requireWrite(t, filepath.Join(s.Dir(), snapshotFileName), "{garbage")
	_, _, err := s.loadSnapshot()
	require.ErrorIs(t, errors.Cause(err), ErrSnapshotCorrupted)
}

func TestFileHandoffStore_LockBusyAndStaleReclaim(t *testing.T) {
	dir := t.TempDir()
	owner, err := openFileHandoffStore(dir, testLogger(), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })

	_, err = openFileHandoffStore(dir, testLogger(), time.Hour)
	require.ErrorIs(t, errors.Cause(err), ErrHandoffStoreLocked)

	// A dead owner (old lock, long TTL seen from the new owner) is reclaimed.
	reclaimer, err := openFileHandoffStore(dir, testLogger(), -time.Second)
	require.NoError(t, err)
	require.NoError(t, reclaimer.Close())
}

func TestFileHandoffStore_ConcurrentSaveSnapshot(t *testing.T) {
	s := newTestStore(t)
	m := newTestManifest(10, validStart(), true, "db.a")
	require.NoError(t, s.SaveSnapshot(m))

	var wg sync.WaitGroup
	var rwm sync.Mutex // read-modify-write serialization at the caller level
	for range 16 {
		wg.Go(func() {
			rwm.Lock()
			defer rwm.Unlock()
			mm, ok, err := s.loadSnapshot()
			require.NoError(t, err)
			require.True(t, ok)
			mm.Tables["db.a"].CompletedBatches++
			require.NoError(t, s.SaveSnapshot(mm))
		})
	}
	wg.Wait()

	got, ok, err := s.loadSnapshot()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 16, got.Tables["db.a"].CompletedBatches)
}

func TestFileHandoffStore_InvalidSaveRejected(t *testing.T) {
	s := newTestStore(t)

	bad := newTestManifest(0, validStart(), true, "db.a")
	err := s.SaveSnapshot(bad)
	require.Error(t, err)

	badRecord := &handoffRecord{Version: handoffVersion, SnapshotComplete: false}
	require.Error(t, s.CommitHandoff(badRecord))

	_, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.False(t, exists)
}

// ---------------------------------------------------------------------------
// Engine fakes
// ---------------------------------------------------------------------------

type sunkBatch struct {
	seq  uint64
	rows []string
}

type recordingApplier struct {
	mu          sync.Mutex
	begins      []string
	ends        []string
	batches     map[string][]sunkBatch
	failBatches map[string]map[uint64]bool
	beginErr    error
	// afterBatch runs after a batch is recorded, before returning; useful to
	// inject cancellation.
	afterBatch func(key string, seq uint64)
}

func newRecordingApplier() *recordingApplier {
	return &recordingApplier{batches: map[string][]sunkBatch{}, failBatches: map[string]map[uint64]bool{}}
}

func (a *recordingApplier) onSnapshotTableBegin(db, table string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.begins = append(a.begins, db+"."+table)
	return a.beginErr
}

func (a *recordingApplier) onSnapshotTableEnd(db, table string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ends = append(a.ends, db+"."+table)
	return nil
}

func (a *recordingApplier) applySnapshotBatch(db, table string, batch uint64, rows [][]string) error {
	key := db + "." + table
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failBatches[key] != nil && a.failBatches[key][batch] {
		return errors.Errorf("downstream failure on %s batch %d", key, batch)
	}
	flat := make([]string, 0, len(rows))
	for _, r := range rows {
		flat = append(flat, strings.Join(r, ","))
	}
	a.batches[key] = append(a.batches[key], sunkBatch{seq: batch, rows: flat})
	if a.afterBatch != nil {
		a.afterBatch(key, batch)
	}
	return nil
}

func (a *recordingApplier) sunk(key string) []sunkBatch {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]sunkBatch(nil), a.batches[key]...)
}

func (a *recordingApplier) snapshot() (begins, ends []string, batches map[string][]sunkBatch) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.begins...), append([]string(nil), a.ends...), cloneBatches(a.batches)
}

func cloneBatches(in map[string][]sunkBatch) map[string][]sunkBatch {
	out := make(map[string][]sunkBatch, len(in))
	for k, v := range in {
		cp := make([]sunkBatch, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

type tableCoord struct {
	pos  handoffStartPoint
	gtid string
}

type fakeTableSource struct {
	mu     sync.Mutex
	rows   map[string][][]string
	coords map[string]tableCoord
	calls  []string
	// failAfterRows kills the dump of a table after that many rows streamed.
	failAfterRows map[string]int
}

func newFakeTableSource(rows map[string][][]string, coords map[string]tableCoord) *fakeTableSource {
	return &fakeTableSource{rows: rows, coords: coords, failAfterRows: map[string]int{}}
}

func (f *fakeTableSource) dump(_ context.Context, db, table string, h dump.ParseHandler) error {
	key := db + "." + table
	f.mu.Lock()
	f.calls = append(f.calls, key)
	coord, hasCoord := f.coords[key]
	limit, hasLimit := f.failAfterRows[key]
	rows := f.rows[key]
	f.mu.Unlock()

	if hasCoord {
		if coord.gtid != "" {
			if err := h.GtidSet(coord.gtid); err != nil {
				return err
			}
		}
		if coord.pos.valid() {
			if err := h.BinLog(coord.pos.BinlogFile, uint64(coord.pos.BinlogPos)); err != nil {
				return err
			}
		}
	}
	for i, r := range rows {
		if hasLimit && i >= limit {
			return errors.Errorf("simulated kill while dumping %s", key)
		}
		if err := h.Data(db, table, r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeTableSource) dumpCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeTableSource) asSource() snapshotTableSource {
	return f.dump
}

func planFor(keys ...string) []dumpTableRef {
	refs := make([]dumpTableRef, 0, len(keys))
	for _, key := range keys {
		refs = append(refs, tableRefFromKey(key))
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].key() < refs[j].key() })
	return refs
}

func numericRows(n int, prefix string) [][]string {
	rows := make([][]string, n)
	for i := range n {
		rows[i] = []string{prefix + "-" + string(rune('a'+i%26)) + string(rune('0'+i/26))}
	}
	return rows
}

func newTestEngine(s *FileHandoffStore, batchSize int, source snapshotTableSource, applier snapshotApplier) *snapshotEngine {
	return &snapshotEngine{
		flavor:    "mysql",
		batchSize: batchSize,
		store:     s,
		source:    source,
		applier:   applier,
		log:       testLogger(),
		now:       func() time.Time { return time.Unix(1700000123, 0) },
	}
}

// ---------------------------------------------------------------------------
// Engine scenarios
// ---------------------------------------------------------------------------

func TestSnapshotEngine_FreshRunCommitsHandoff(t *testing.T) {
	s := newTestStore(t)
	coords := map[string]tableCoord{
		"db.a": {
			pos:  handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 154},
			gtid: "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa:1-10",
		},
		// a newer coordinate from a later per-table dump must be ignored
		"db.b": {pos: handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 9999}},
	}
	source := newFakeTableSource(map[string][][]string{
		"db.a": numericRows(3, "a"),
		"db.b": numericRows(1, "b"),
	}, coords)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)

	rec, resumed, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a", "db.b"))
	require.NoError(t, err)
	require.False(t, resumed)
	require.True(t, rec.SnapshotComplete)
	require.Equal(t, uint32(154), rec.Start.BinlogPos)
	require.Equal(t, "mysql-bin.000001", rec.Start.BinlogFile)
	require.Equal(t, "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa:1-10", rec.Start.GTIDSet)
	require.Equal(t, 2, rec.Tables)
	require.EqualValues(t, 3, rec.CompletedBatches) // 2 batches for a (2+1 rows), 1 for b

	sunkA := applier.sunk("db.a")
	require.Equal(t, []sunkBatch{
		{seq: 1, rows: []string{"a-a0", "a-b0"}},
		{seq: 2, rows: []string{"a-c0"}},
	}, sunkA)
	require.Equal(t, []sunkBatch{{seq: 1, rows: []string{"b-a0"}}}, applier.sunk("db.b"))

	begins, ends, _ := applier.snapshot()
	require.Equal(t, []string{"db.a", "db.b"}, begins)
	require.Equal(t, []string{"db.a", "db.b"}, ends)
	require.Equal(t, []string{"db.a", "db.b"}, source.dumpCalls())

	// The handoff on disk validates and matches the manifest.
	loaded, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, rec.Start, loaded.Start)
}

func TestSnapshotEngine_ResumeSkipsCompletedTablesAndBatches(t *testing.T) {
	dir := t.TempDir()

	coords := map[string]tableCoord{
		"db.a": {pos: handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 154}},
		"db.b": {pos: handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 9999}},
	}
	rows := map[string][][]string{
		"db.a": numericRows(2, "a"),
		"db.b": numericRows(5, "b"),
	}

	// Run 1: killed after streaming 3 rows of db.b: batch 1 (rows 1-2) is
	// committed, row 3 is in the lost in-flight batch.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		source := newFakeTableSource(rows, coords)
		source.failAfterRows["db.b"] = 3
		applier := newRecordingApplier()
		eng := newTestEngine(s, 2, source.asSource(), applier)
		_, _, err = eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a", "db.b"))
		require.Error(t, err)

		require.Equal(t, 1, len(applier.sunk("db.b"))) // only batch 1 made it
		require.NoError(t, s.Close())
	}

	// Run 2: resume. db.a must not be re-dumped; db.b re-streams from row 1
	// but committed rows are fast-forwarded without delivery; the lost
	// in-flight batch is redelivered wholesale (rows 3-4), then row 5.
	s2 := reopenTestStore(t, dir)
	source2 := newFakeTableSource(rows, coords) // no failure this time
	applier2 := newRecordingApplier()
	eng2 := newTestEngine(s2, 2, source2.asSource(), applier2)
	rec, resumed, err := eng2.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a", "db.b"))
	require.NoError(t, err)
	require.False(t, resumed)
	require.True(t, rec.SnapshotComplete)
	require.Equal(t, uint32(154), rec.Start.BinlogPos) // start point never moved

	require.Equal(t, []string{"db.b"}, source2.dumpCalls(), "completed table must be skipped")

	sunkB := applier2.sunk("db.b")
	require.Equal(t, []sunkBatch{
		{seq: 2, rows: []string{"b-c0", "b-d0"}}, // redelivered batch
		{seq: 3, rows: []string{"b-e0"}},
	}, sunkB)
	require.Empty(t, applier2.sunk("db.a"), "completed table rows must not be redelivered")

	// begin/end only fire for the table actually resumed.
	begins, ends, _ := applier2.snapshot()
	require.Equal(t, []string{"db.b"}, begins)
	require.Equal(t, []string{"db.b"}, ends)
}

func TestSnapshotEngine_HandoffGateSkipsDump(t *testing.T) {
	dir := t.TempDir()

	// Complete a snapshot first.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		source := newFakeTableSource(
			map[string][][]string{"db.a": numericRows(1, "a")},
			map[string]tableCoord{"db.a": {pos: validStart()}})
		applier := newRecordingApplier()
		eng := newTestEngine(s, 1, source.asSource(), applier)
		_, _, err = eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}

	// Restart: the existing handoff is the only thing consulted; the source
	// is never invoked and the start point is returned for binlog sync.
	s2 := reopenTestStore(t, dir)
	source2 := newFakeTableSource(nil, nil)
	applier2 := newRecordingApplier()
	eng2 := newTestEngine(s2, 1, source2.asSource(), applier2)
	rec, resumed, err := eng2.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.NoError(t, err)
	require.True(t, resumed)
	require.True(t, rec.SnapshotComplete)
	require.Equal(t, validStart(), rec.Start)
	require.Empty(t, source2.dumpCalls())
	begins, ends, batches := applier2.snapshot()
	require.Empty(t, begins)
	require.Empty(t, ends)
	require.Empty(t, batches)
}

func TestSnapshotEngine_CorruptHandoffNeverEntersIncremental(t *testing.T) {
	s := newTestStore(t)
	// Committed manifest but a half-written handoff file.
	m := newTestManifest(2, validStart(), true, "db.a")
	m.Tables["db.a"].State = snapshotStateDone
	m.Tables["db.a"].CompletedBatches = 1
	require.NoError(t, s.SaveSnapshot(m))
	requireWrite(t, filepath.Join(s.Dir(), handoffFileName), `{"version":1,"snapshot_complete":true`)

	source := newFakeTableSource(nil, nil)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	_, resumed, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.Error(t, err)
	require.False(t, resumed)
	require.ErrorIs(t, errors.Cause(err), ErrIncompleteHandoff)
	require.Empty(t, source.dumpCalls(), "no dump may run against an unreadable handoff")
}

func TestSnapshotEngine_DownstreamFailureRedeliversBatch(t *testing.T) {
	dir := t.TempDir()
	coords := map[string]tableCoord{"db.a": {pos: validStart()}}
	rows := map[string][][]string{"db.a": numericRows(4, "a")}

	// Batch 2 fails downstream: it must not be marked complete.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		source := newFakeTableSource(rows, coords)
		applier := newRecordingApplier()
		applier.failBatches["db.a"] = map[uint64]bool{2: true}
		eng := newTestEngine(s, 2, source.asSource(), applier)
		_, _, err = eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
		require.Error(t, err)
		require.NoError(t, s.Close())
	}

	{
		s := reopenTestStore(t, dir)
		source := newFakeTableSource(rows, coords)
		applier := newRecordingApplier()
		eng := newTestEngine(s, 2, source.asSource(), applier)
		_, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
		require.NoError(t, err)

		// seq 1 fast-forwarded; seq 2 delivered whole on resume; then seq 3.
		require.Equal(t, []sunkBatch{
			{seq: 2, rows: []string{"a-c0", "a-d0"}},
		}, applier.sunk("db.a"))
	}
}

func TestSnapshotEngine_EmptyTableCompletes(t *testing.T) {
	s := newTestStore(t)
	source := newFakeTableSource(
		map[string][][]string{"db.a": {}, "db.b": numericRows(1, "b")},
		map[string]tableCoord{
			"db.a": {pos: validStart()},
			"db.b": {pos: handoffStartPoint{BinlogFile: "mysql-bin.000001", BinlogPos: 9999}},
		})
	applier := newRecordingApplier()
	eng := newTestEngine(s, 3, source.asSource(), applier)
	rec, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a", "db.b"))
	require.NoError(t, err)
	require.Equal(t, 2, rec.Tables) // both planned tables, even the empty one
	require.EqualValues(t, 1, rec.CompletedBatches)
	require.Nil(t, applier.sunk("db.a"))

	m, exists, err := s.loadSnapshot()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, snapshotStateDone, m.Tables["db.a"].State)
}

func TestSnapshotEngine_ShrunkTableFailsClosed(t *testing.T) {
	dir := t.TempDir()
	// Ledger claims 2 committed batches (4 rows); on resume the table only
	// streams 3 rows, so batch boundaries can no longer align.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		m := newTestManifest(2, validStart(), true, "db.a")
		m.Tables["db.a"].State = snapshotStateDumping
		m.Tables["db.a"].CompletedBatches = 2
		require.NoError(t, s.SaveSnapshot(m))
		require.NoError(t, s.Close())
	}
	s := reopenTestStore(t, dir)
	source := newFakeTableSource(
		map[string][][]string{"db.a": numericRows(3, "a")},
		map[string]tableCoord{"db.a": {pos: validStart()}})
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	_, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.ErrorIs(t, errors.Cause(err), ErrSnapshotShrunk)

	_, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.False(t, exists, "no handoff must be committed after shrink")
}

func TestSnapshotEngine_ProgressWithoutStartPointFailsClosed(t *testing.T) {
	dir := t.TempDir()
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		// batches delivered but the coordinate callback never persisted.
		m := newTestManifest(2, handoffStartPoint{}, false, "db.a")
		m.Tables["db.a"].CompletedBatches = 1
		require.NoError(t, s.SaveSnapshot(m))
		require.NoError(t, s.Close())
	}
	s := reopenTestStore(t, dir)
	source := newFakeTableSource(
		map[string][][]string{"db.a": numericRows(4, "a")}, nil)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	_, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.ErrorIs(t, errors.Cause(err), ErrStartPointMissing)
	require.Empty(t, source.dumpCalls())
}

func TestSnapshotEngine_NoCoordinateFromDumpFailsClosed(t *testing.T) {
	// Fresh snapshot, source never reports a binlog coordinate: no handoff
	// may be committed and incremental sync must not start.
	s := newTestStore(t)
	source := newFakeTableSource(
		map[string][][]string{"db.a": numericRows(2, "a")}, nil)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	_, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.ErrorIs(t, errors.Cause(err), ErrStartPointMissing)

	_, exists, err := s.loadHandoff()
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSnapshotEngine_GTIDOnlyStartAccepted(t *testing.T) {
	s := newTestStore(t)
	start := handoffStartPoint{GTIDSet: "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa:1-3"}
	source := newFakeTableSource(
		map[string][][]string{"db.a": numericRows(1, "a")}, nil)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	rec, _, err := eng.Run(context.Background(), start, true, planFor("db.a"))
	require.NoError(t, err)
	require.Equal(t, start.GTIDSet, rec.Start.GTIDSet)
	require.Zero(t, rec.Start.BinlogPos)
}

func TestSnapshotEngine_BatchSizeChangeRejected(t *testing.T) {
	dir := t.TempDir()
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		m := newTestManifest(2, validStart(), true, "db.a")
		require.NoError(t, s.SaveSnapshot(m))
		require.NoError(t, s.Close())
	}
	s := reopenTestStore(t, dir)
	source := newFakeTableSource(nil, nil)
	eng := newTestEngine(s, 3, source.asSource(), newRecordingApplier())
	_, _, err := eng.Run(context.Background(), validStart(), true, planFor("db.a"))
	require.ErrorIs(t, errors.Cause(err), ErrSnapshotCorrupted)
}

func TestSnapshotEngine_ContextCancelIsResumable(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	coords := map[string]tableCoord{"db.a": {pos: validStart()}}
	rows := map[string][][]string{"db.a": numericRows(5, "a")}

	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		source := newFakeTableSource(rows, coords)
		// cancel right after the first committed batch is delivered.
		applier := newRecordingApplier()
		applier.afterBatch = func(_ string, seq uint64) {
			if seq == 1 {
				cancel()
			}
		}
		eng := newTestEngine(s, 2, source.asSource(), applier)
		_, _, err = eng.Run(ctx, handoffStartPoint{}, false, planFor("db.a"))
		require.Error(t, err)
		require.NoError(t, s.Close())
	}

	s := reopenTestStore(t, dir)
	source2 := newFakeTableSource(rows, coords)
	applier2 := newRecordingApplier()
	eng2 := newTestEngine(s, 2, source2.asSource(), applier2)
	rec, _, err := eng2.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.NoError(t, err)
	require.True(t, rec.SnapshotComplete)
	// rows 1-2 skipped; batch 2 (rows 3-4) redelivered; batch 3 (row 5).
	require.Equal(t, []sunkBatch{
		{seq: 2, rows: []string{"a-c0", "a-d0"}},
		{seq: 3, rows: []string{"a-e0"}},
	}, applier2.sunk("db.a"))
}

func TestSnapshotEngine_AllTablesDoneButHandoffMissingCommitsDirectly(t *testing.T) {
	dir := t.TempDir()
	// Crash window after the last table completed but before (or during) the
	// handoff write: all data is downstream, resume only needs to commit the
	// handoff, without re-dumping anything.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		m := newTestManifest(2, validStart(), true, "db.a", "db.b")
		for _, p := range m.Tables {
			p.State = snapshotStateDone
			p.CompletedBatches = 1
		}
		require.NoError(t, s.SaveSnapshot(m))
		require.NoError(t, s.Close())
	}
	s := reopenTestStore(t, dir)
	source := newFakeTableSource(nil, nil)
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	rec, _, err := eng.Run(context.Background(), validStart(), true, planFor("db.a", "db.b"))
	require.NoError(t, err)
	require.True(t, rec.SnapshotComplete)
	require.Equal(t, 2, rec.Tables)
	require.EqualValues(t, 2, rec.CompletedBatches)
	require.Empty(t, source.dumpCalls())
	begins, ends, _ := applier.snapshot()
	require.Empty(t, begins)
	require.Empty(t, ends)
}

func TestSnapshotEngine_ResumeAfterEarlyCrashAdoptsFreshStart(t *testing.T) {
	dir := t.TempDir()
	// Crash after manifest creation but before the dump stream emitted its
	// coordinate and before any batch landed: resume with a fresh start point.
	{
		s, err := openFileHandoffStore(dir, testLogger(), time.Millisecond)
		require.NoError(t, err)
		m := newTestManifest(2, handoffStartPoint{}, false, "db.a")
		require.NoError(t, s.SaveSnapshot(m))
		require.NoError(t, s.Close())
	}
	s := reopenTestStore(t, dir)
	source := newFakeTableSource(
		map[string][][]string{"db.a": numericRows(2, "a")},
		map[string]tableCoord{"db.a": {pos: validStart()}})
	applier := newRecordingApplier()
	eng := newTestEngine(s, 2, source.asSource(), applier)
	rec, _, err := eng.Run(context.Background(), handoffStartPoint{}, false, planFor("db.a"))
	require.NoError(t, err)
	require.Equal(t, validStart(), rec.Start)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func requireWrite(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func requireWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	require.NoError(t, writeJSONAtomic(path, v))
}
