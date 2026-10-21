package canal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/dump"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
)

// Snapshot/incremental handoff state machine.
//
// The initial dump is executed table by table and every table is streamed
// in fixed-size batches. Both the snapshot progress and the binlog start
// point are persisted durably so that a process kill halfway through the
// dump is recoverable:
//
//   - snapshot.json records the dump plan, the per-table batch progress and
//     the binlog/GTID start point captured for the whole snapshot.
//   - handoff.json is written once, atomically, only after every table of
//     the plan is fully delivered. It carries the snapshot-complete marker
//     together with the incremental start point. Incremental sync is allowed
//     to start only when this record exists AND validates.
//
// Delivery semantics are at-least-once: the batch in flight when the process
// died is re-delivered on resume, so the downstream EventHandler must be
// idempotent, exactly as it already has to be for binlog replay.
const (
	handoffVersion = 1

	snapshotStatePending = "pending"
	snapshotStateDumping = "dumping"
	snapshotStateDone    = "done"

	snapshotFileName = "snapshot.json"
	handoffFileName  = "handoff.json"
	lockFileName     = "lock"

	// defaultSnapshotBatchSize is the number of rows delivered in one snapshot
	// batch when Dump.BatchSize is not configured.
	defaultSnapshotBatchSize = 1000
	// defaultLockTTL reclaims a lock file older than that, assuming its owner
	// was killed (killed processes get no chance to release it).
	defaultLockTTL = time.Minute
)

var (
	// ErrIncompleteHandoff means the snapshot->incremental handoff record is
	// missing, corrupted or only partially written. Canal fails closed and
	// refuses to start the binlog syncer, because starting incremental sync
	// from an unknown point would either duplicate or lose changes.
	ErrIncompleteHandoff = errors.New("handoff record is incomplete or corrupted, refusing to enter incremental sync")
	// ErrSnapshotCorrupted means the resumable snapshot manifest is not
	// readable or fails validation.
	ErrSnapshotCorrupted = errors.New("snapshot progress record is corrupted")
	// ErrStartPointMissing means snapshot data was already delivered but the
	// binlog start point was never persisted; the snapshot cannot be resumed
	// safely.
	ErrStartPointMissing = errors.New("snapshot has delivered batches but no binlog start point was recorded")
	// ErrSnapshotShrunk means a table re-dumped on resume produced fewer rows
	// than the batches already marked complete, so batch boundaries can no
	// longer be aligned.
	ErrSnapshotShrunk = errors.New("table dump shrunk below already completed batches, cannot resume snapshot")
	// ErrHandoffStoreLocked means another canal instance owns the state
	// directory.
	ErrHandoffStoreLocked = errors.New("snapshot state directory is already in use")
)

// handoffStartPoint is the binlog coordinate incremental replication starts
// from once the snapshot is complete. GTIDSet is a serialized mysql.GTIDSet
// and may be empty when replicating by binlog file/position only.
type handoffStartPoint struct {
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint32 `json:"binlog_pos"`
	GTIDSet    string `json:"gtid_set,omitempty"`
}

func (p handoffStartPoint) valid() bool {
	return p.BinlogPos > 0 || p.GTIDSet != ""
}

func (p handoffStartPoint) equal(o handoffStartPoint) bool {
	return p.BinlogFile == o.BinlogFile && p.BinlogPos == o.BinlogPos && p.GTIDSet == o.GTIDSet
}

func (p handoffStartPoint) position() mysql.Position {
	return mysql.Position{Name: p.BinlogFile, Pos: p.BinlogPos}
}

func (p handoffStartPoint) gtidSet(flavor string) (mysql.GTIDSet, error) {
	if p.GTIDSet == "" {
		return nil, nil
	}
	return mysql.ParseGTIDSet(flavor, p.GTIDSet)
}

type snapshotTableProgress struct {
	// State is pending, dumping or done.
	State string `json:"state"`
	// CompletedBatches counts batches durably delivered downstream.
	// Batches are 1-numbered: CompletedBatches=n means batches 1..n are safe
	// to skip on resume and batch n+1 is the first one to (re-)deliver.
	CompletedBatches uint64 `json:"completed_batches"`
}

func (p *snapshotTableProgress) validate() error {
	if p == nil {
		return errors.New("nil table progress")
	}
	switch p.State {
	case snapshotStatePending, snapshotStateDumping, snapshotStateDone:
	default:
		return errors.Errorf("unknown snapshot table state %q", p.State)
	}
	return nil
}

// snapshotManifest is the resumable progress of the initial dump.
type snapshotManifest struct {
	Version   int    `json:"version"`
	Flavor    string `json:"flavor"`
	BatchSize int    `json:"batch_size"`
	// Start is the binlog/GTID coordinate captured for this snapshot; it
	// becomes the incremental start point at handoff.
	Start         handoffStartPoint                 `json:"start"`
	StartCaptured bool                              `json:"start_captured"`
	Tables        map[string]*snapshotTableProgress `json:"tables"`
	CreatedAt     time.Time                         `json:"created_at"`
	UpdatedAt     time.Time                         `json:"updated_at"`
}

func (m *snapshotManifest) validate() error {
	if m == nil {
		return ErrSnapshotCorrupted
	}
	if m.Version != handoffVersion {
		return errors.Wrapf(ErrSnapshotCorrupted, "unsupported version %d", m.Version)
	}
	if m.BatchSize <= 0 {
		return errors.Wrap(ErrSnapshotCorrupted, "invalid batch size")
	}
	if m.Tables == nil {
		return errors.Wrap(ErrSnapshotCorrupted, "missing table plan")
	}
	if m.StartCaptured && !m.Start.valid() {
		return errors.Wrap(ErrSnapshotCorrupted, "captured start point is empty")
	}
	for key, p := range m.Tables {
		if p == nil {
			return errors.Wrapf(ErrSnapshotCorrupted, "missing progress for table %s", key)
		}
		if err := p.validate(); err != nil {
			return errors.Wrapf(ErrSnapshotCorrupted, "table %s: %v", key, err)
		}
	}
	return nil
}

func (m *snapshotManifest) hasProgress() bool {
	for _, p := range m.Tables {
		if p.CompletedBatches > 0 || p.State == snapshotStateDone {
			return true
		}
	}
	return false
}

func (m *snapshotManifest) allTablesDone() bool {
	for _, p := range m.Tables {
		if p.State != snapshotStateDone {
			return false
		}
	}
	return true
}

func (m *snapshotManifest) completedBatches() uint64 {
	var total uint64
	for _, p := range m.Tables {
		total += p.CompletedBatches
	}
	return total
}

func (m *snapshotManifest) sortedTableKeys() []string {
	keys := make([]string, 0, len(m.Tables))
	for key := range m.Tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// handoffRecord is the atomic snapshot->incremental handoff. The snapshot
// completion marker and the incremental start point only ever exist together.
type handoffRecord struct {
	Version          int               `json:"version"`
	Flavor           string            `json:"flavor"`
	Start            handoffStartPoint `json:"start"`
	SnapshotComplete bool              `json:"snapshot_complete"`
	Tables           int               `json:"tables"`
	CompletedBatches uint64            `json:"completed_batches"`
	CompletedAt      time.Time         `json:"completed_at"`
}

func (r *handoffRecord) validate() error {
	if r == nil {
		return errors.Wrap(ErrIncompleteHandoff, "missing record")
	}
	if r.Version != handoffVersion {
		return errors.Wrapf(ErrIncompleteHandoff, "unsupported version %d", r.Version)
	}
	if !r.SnapshotComplete {
		return errors.Wrap(ErrIncompleteHandoff, "snapshot not marked complete")
	}
	if !r.Start.valid() {
		return errors.Wrap(ErrIncompleteHandoff, "missing incremental start point")
	}
	if r.Tables < 0 {
		return errors.Wrap(ErrIncompleteHandoff, "invalid table count")
	}
	if r.CompletedAt.IsZero() {
		return errors.Wrap(ErrIncompleteHandoff, "missing completion timestamp")
	}
	return nil
}

// lockFile is an advisory single-writer guard. A killed process leaves it on
// disk; it is reclaimed once it is older than the TTL.
type lockFile struct {
	Nonce string    `json:"nonce"`
	PID   int       `json:"pid"`
	At    time.Time `json:"at"`
}

// FileHandoffStore persists snapshot progress and the handoff record as JSON
// files inside one directory. Writes are atomic (temp file + fsync + rename)
// so a crash can never leave a half-written committed record.
type FileHandoffStore struct {
	dir string
	log *slog.Logger
	now func() time.Time

	lockTTL time.Duration
	nonce   string

	mu     sync.Mutex
	closed bool
}

// OpenFileHandoffStore opens (creating if needed) the state directory and
// acquires its single-writer lock.
func OpenFileHandoffStore(dir string, logger *slog.Logger) (*FileHandoffStore, error) {
	return openFileHandoffStore(dir, logger, defaultLockTTL)
}

func openFileHandoffStore(dir string, logger *slog.Logger, lockTTL time.Duration) (*FileHandoffStore, error) {
	if dir == "" {
		return nil, errors.New("empty snapshot state directory")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Trace(err)
	}

	s := &FileHandoffStore{
		dir:     dir,
		log:     logger,
		now:     utils.Now,
		lockTTL: lockTTL,
		nonce:   newLockNonce(),
	}
	if err := s.acquireLock(); err != nil {
		return nil, err
	}
	return s, nil
}

// Dir returns the backing directory.
func (s *FileHandoffStore) Dir() string {
	return s.dir
}

func (s *FileHandoffStore) acquireLock() error {
	path := filepath.Join(s.dir, lockFileName)

	stale := false
	if data, err := os.ReadFile(path); err == nil {
		last := lockFile{}
		if jsonErr := json.Unmarshal(data, &last); jsonErr != nil || last.At.IsZero() || s.now().Sub(last.At) <= s.lockTTL {
			// Either an unreadable lock (treat as held) or a fresh one.
			return errors.Wrapf(ErrHandoffStoreLocked, "lock at %s", path)
		}
		stale = true
	} else if !stderrors.Is(err, fs.ErrNotExist) {
		return errors.Trace(err)
	}

	if stale {
		s.log.Warn("reclaiming stale snapshot lock", slog.String("path", path))
		if err := os.Remove(path); err != nil && !stderrors.Is(err, fs.ErrNotExist) {
			return errors.Trace(err)
		}
	}

	rec := lockFile{Nonce: s.nonce, PID: os.Getpid(), At: s.now()}
	data, err := json.Marshal(&rec)
	if err != nil {
		return errors.Trace(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if stderrors.Is(err, fs.ErrExist) {
			return errors.Wrapf(ErrHandoffStoreLocked, "lock at %s", path)
		}
		return errors.Trace(err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return errors.Trace(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.Trace(err)
	}
	return errors.Trace(f.Close())
}

// Close releases the lock. It does not touch snapshot or handoff records.
func (s *FileHandoffStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	path := filepath.Join(s.dir, lockFileName)
	if data, err := os.ReadFile(path); err == nil {
		last := lockFile{}
		if json.Unmarshal(data, &last) == nil && last.Nonce == s.nonce {
			if err := os.Remove(path); err != nil && !stderrors.Is(err, fs.ErrNotExist) {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (s *FileHandoffStore) snapshotPath() string {
	return filepath.Join(s.dir, snapshotFileName)
}

func (s *FileHandoffStore) handoffPath() string {
	return filepath.Join(s.dir, handoffFileName)
}

// loadSnapshot reads the resumable snapshot manifest.
// A missing file is reported as (nil, false, nil).
func (s *FileHandoffStore) loadSnapshot() (*snapshotManifest, bool, error) {
	data, err := os.ReadFile(s.snapshotPath())
	if err != nil {
		if stderrors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, errors.Trace(err)
	}
	m := new(snapshotManifest)
	if err := json.Unmarshal(data, m); err != nil {
		return nil, false, errors.Wrap(ErrSnapshotCorrupted, err.Error())
	}
	if err := m.validate(); err != nil {
		return nil, false, errors.Trace(err)
	}
	return m, true, nil
}

// SaveSnapshot persists the manifest atomically.
func (s *FileHandoffStore) SaveSnapshot(m *snapshotManifest) error {
	if err := m.validate(); err != nil {
		return errors.Trace(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	m.UpdatedAt = now
	return writeJSONAtomic(s.snapshotPath(), m)
}

// loadHandoff reads and validates the committed handoff record.
// A missing file is reported as (nil, false, nil). Any record that cannot be
// validated, or that does not match the completed snapshot manifest, yields
// ErrIncompleteHandoff: callers must fail closed and must not start binlog
// sync.
func (s *FileHandoffStore) loadHandoff() (*handoffRecord, bool, error) {
	data, err := os.ReadFile(s.handoffPath())
	if err != nil {
		if stderrors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, errors.Trace(err)
	}
	r := new(handoffRecord)
	if err := json.Unmarshal(data, r); err != nil {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, err.Error())
	}
	if err := r.validate(); err != nil {
		return nil, false, errors.Trace(err)
	}

	// The handoff is only trustworthy together with the matching, fully
	// completed snapshot manifest.
	m, exists, err := s.loadSnapshot()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !exists {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, "snapshot manifest is missing")
	}
	if !m.StartCaptured || !m.Start.equal(r.Start) {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, "start point does not match snapshot manifest")
	}
	if !m.allTablesDone() {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, "snapshot manifest still has unfinished tables")
	}
	if r.Tables != len(m.Tables) || r.CompletedBatches != m.completedBatches() {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, "batch/table counters do not match snapshot manifest")
	}
	return r, true, nil
}

// CommitHandoff atomically writes the handoff record. A crash during the
// write leaves either the previous (absent) file or the complete new one.
func (s *FileHandoffStore) CommitHandoff(r *handoffRecord) error {
	if err := r.validate(); err != nil {
		return errors.Trace(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.handoffPath(), r)
}

// writeJSONAtomic writes v to path via a temp file in the same directory,
// fsyncs it and renames it into place, then fsyncs the directory.
func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}

	dir := filepath.Dir(path)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return errors.Trace(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.Trace(err)
	}
	if err := f.Close(); err != nil {
		return errors.Trace(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return errors.Trace(err)
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// dumpTableRef identifies a table in the dump plan.
type dumpTableRef struct {
	db    string
	table string
}

func (r dumpTableRef) key() string {
	return fmt.Sprintf("%s.%s", r.db, r.table)
}

// snapshotTableSource streams one table dump into the parse handler.
type snapshotTableSource func(ctx context.Context, db, table string, h dump.ParseHandler) error

// snapshotApplier delivers snapshot data downstream. Batch delivery happens
// before the batch is durably marked complete, so an error anywhere leaves
// the batch uncommitted and eligible for redelivery on resume.
type snapshotApplier interface {
	onSnapshotTableBegin(db, table string) error
	applySnapshotBatch(db, table string, batch uint64, rows [][]string) error
	onSnapshotTableEnd(db, table string) error
}

// snapshotEngine runs a resumable per-table, per-batch snapshot and commits
// the snapshot/incremental handoff exactly once.
type snapshotEngine struct {
	flavor    string
	batchSize int
	store     *FileHandoffStore
	source    snapshotTableSource
	applier   snapshotApplier
	log       *slog.Logger
	now       func() time.Time

	// manifest is non-nil while Run is in progress.
	manifest *snapshotManifest
}

// Run executes the snapshot. When a valid handoff already exists it is
// returned with resumed=true without touching the source: the caller seeds
// the binlog syncer from its start point and skips the dump entirely.
func (e *snapshotEngine) Run(ctx context.Context, initial handoffStartPoint, initialCaptured bool, plan []dumpTableRef) (record *handoffRecord, resumed bool, err error) {
	// Gate #1: a committed handoff is the only way into incremental sync.
	record, exists, err := e.store.loadHandoff()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if exists {
		e.log.Info("snapshot handoff already committed, skip dump", slog.Any("start", record.Start))
		return record, true, nil
	}

	m, exists, err := e.store.loadSnapshot()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if !exists {
		m, err = e.newManifest(initial, initialCaptured, plan)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if err := e.store.SaveSnapshot(m); err != nil {
			return nil, false, errors.Trace(err)
		}
		e.log.Info("started resumable snapshot",
			slog.Int("tables", len(m.Tables)),
			slog.Int("batch_size", m.BatchSize),
			slog.Bool("start_captured", m.StartCaptured))
	} else {
		// Data may already be downstream. Without a recorded start point we
		// can never derive the binlog gap, so fail closed.
		if m.hasProgress() && !m.StartCaptured {
			return nil, false, errors.Trace(ErrStartPointMissing)
		}
		if !m.StartCaptured && initialCaptured {
			// Crash happened before the dump stream produced its coordinate
			// and nothing was delivered; adopt the freshly captured one.
			m.Start = initial
			m.StartCaptured = true
			if err := e.store.SaveSnapshot(m); err != nil {
				return nil, false, errors.Trace(err)
			}
		}
		if m.BatchSize != e.batchSize {
			return nil, false, errors.Wrapf(ErrSnapshotCorrupted,
				"batch size changed mid snapshot: ledger=%d config=%d", m.BatchSize, e.batchSize)
		}
		e.log.Info("resuming snapshot",
			slog.Int("tables", len(m.Tables)),
			slog.Uint64("completed_batches", m.completedBatches()))
	}
	e.manifest = m

	for _, key := range m.sortedTableKeys() {
		if err := ctx.Err(); err != nil {
			return nil, false, errors.Trace(err)
		}
		prog := m.Tables[key]
		if prog.State == snapshotStateDone {
			e.log.Info("snapshot table already complete, skip", slog.String("table", key))
			continue
		}
		ref := tableRefFromKey(key)
		if err := e.dumpTable(ctx, ref, prog); err != nil {
			return nil, false, errors.Trace(err)
		}
	}

	// Gate #2: never hand off an unfinished snapshot or one without a start
	// point. Either failure keeps us out of incremental sync.
	if !m.allTablesDone() {
		return nil, false, errors.Wrap(ErrIncompleteHandoff, "snapshot still has unfinished tables")
	}
	if !m.StartCaptured || !m.Start.valid() {
		return nil, false, errors.Trace(ErrStartPointMissing)
	}

	record = &handoffRecord{
		Version:          handoffVersion,
		Flavor:           e.flavor,
		Start:            m.Start,
		SnapshotComplete: true,
		Tables:           len(m.Tables),
		CompletedBatches: m.completedBatches(),
		CompletedAt:      e.now(),
	}
	if err := e.store.CommitHandoff(record); err != nil {
		return nil, false, errors.Trace(err)
	}
	e.log.Info("snapshot complete, handoff committed",
		slog.Any("start", m.Start),
		slog.Int("tables", len(m.Tables)),
		slog.Uint64("batches", record.CompletedBatches))
	return record, false, nil
}

func (e *snapshotEngine) newManifest(initial handoffStartPoint, initialCaptured bool, plan []dumpTableRef) (*snapshotManifest, error) {
	if initialCaptured && !initial.valid() {
		return nil, errors.New("captured snapshot start point is empty")
	}
	tables := make(map[string]*snapshotTableProgress, len(plan))
	for _, ref := range plan {
		key := ref.key()
		if _, ok := tables[key]; ok {
			return nil, errors.Errorf("duplicate table in dump plan: %s", key)
		}
		tables[key] = &snapshotTableProgress{State: snapshotStatePending}
	}
	now := e.now()
	return &snapshotManifest{
		Version:       handoffVersion,
		Flavor:        e.flavor,
		BatchSize:     e.batchSize,
		Start:         initial,
		StartCaptured: initialCaptured,
		Tables:        tables,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

// observeStartPoint records the binlog/GTID coordinate emitted by the dump
// stream. mysqldump emits the GTID set and the binlog file/position on
// separate lines of the same per-table stream, so each component is accepted
// once (whichever line arrives first); coordinates of later tables never
// overwrite the snapshot start point.
func (e *snapshotEngine) observeStartPoint(p handoffStartPoint) error {
	m := e.manifest
	if m == nil {
		return nil
	}
	if p.BinlogFile != "" && m.Start.BinlogFile == "" {
		m.Start.BinlogFile = p.BinlogFile
		m.Start.BinlogPos = p.BinlogPos
	}
	if p.GTIDSet != "" && m.Start.GTIDSet == "" {
		m.Start.GTIDSet = p.GTIDSet
	}
	if !m.StartCaptured && m.Start.valid() {
		m.StartCaptured = true
		if err := e.store.SaveSnapshot(m); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *snapshotEngine) dumpTable(ctx context.Context, ref dumpTableRef, prog *snapshotTableProgress) error {
	key := ref.key()
	log := e.log.With(slog.String("table", key), slog.Uint64("resume_from_batch", prog.CompletedBatches))
	log.Info("dump snapshot table")

	prog.State = snapshotStateDumping

	if err := e.applier.onSnapshotTableBegin(ref.db, ref.table); err != nil {
		return errors.Trace(err)
	}

	h := &snapshotTableHandler{
		engine:   e,
		ctx:      ctx,
		ref:      ref,
		prog:     prog,
		skipRows: prog.CompletedBatches * uint64(e.batchSize),
		nextSeq:  prog.CompletedBatches + 1,
	}

	srcErr := e.source(ctx, ref.db, ref.table, h)
	if srcErr != nil {
		return errors.Trace(srcErr)
	}
	if err := ctx.Err(); err != nil {
		return errors.Trace(err)
	}
	// The source ended cleanly: the stream must at least cover every row of
	// the batches already marked complete.
	if h.seen < h.skipRows {
		return errors.Wrapf(ErrSnapshotShrunk, "%s: dumped %d rows, %d already committed", key, h.seen, h.skipRows)
	}
	// Deliver the final, possibly short batch.
	if len(h.buf) > 0 {
		if err := h.commitBatch(); err != nil {
			return errors.Trace(err)
		}
	}

	prog.State = snapshotStateDone
	if err := e.store.SaveSnapshot(e.manifest); err != nil {
		return errors.Trace(err)
	}
	if err := e.applier.onSnapshotTableEnd(ref.db, ref.table); err != nil {
		return errors.Trace(err)
	}
	log.Info("snapshot table complete", slog.Uint64("batches", prog.CompletedBatches))
	return nil
}

// snapshotTableHandler is the dump.ParseHandler fed by one per-table dump
// stream. Rows of already completed batches are fast-forwarded; everything
// after the resume point is grouped into batches.
type snapshotTableHandler struct {
	engine *snapshotEngine
	ctx    context.Context
	ref    dumpTableRef
	prog   *snapshotTableProgress

	skipRows uint64
	seen     uint64
	nextSeq  uint64
	buf      [][]string
}

func (h *snapshotTableHandler) BinLog(name string, pos uint64) error {
	if err := h.ctx.Err(); err != nil {
		return errors.Trace(err)
	}
	return h.engine.observeStartPoint(handoffStartPoint{BinlogFile: name, BinlogPos: uint32(pos)})
}

func (h *snapshotTableHandler) GtidSet(gtidSet string) error {
	if err := h.ctx.Err(); err != nil {
		return errors.Trace(err)
	}
	return h.engine.observeStartPoint(handoffStartPoint{GTIDSet: gtidSet})
}

func (h *snapshotTableHandler) Data(_ string, _ string, values []string) error {
	if err := h.ctx.Err(); err != nil {
		return errors.Trace(err)
	}
	h.seen++
	if h.seen <= h.skipRows {
		// Row belongs to a batch already committed before the crash; do not
		// deliver it again.
		return nil
	}

	row := make([]string, len(values))
	copy(row, values)
	h.buf = append(h.buf, row)

	if len(h.buf) >= h.engine.batchSize {
		return h.commitBatch()
	}
	return nil
}

// commitBatch delivers one batch downstream and only then durably marks it
// complete. A crash between the two redelivers the batch (at-least-once).
func (h *snapshotTableHandler) commitBatch() error {
	seq := h.nextSeq
	rows := h.buf
	if err := h.engine.applier.applySnapshotBatch(h.ref.db, h.ref.table, seq, rows); err != nil {
		return errors.Trace(err)
	}
	h.prog.CompletedBatches = seq
	if err := h.engine.store.SaveSnapshot(h.engine.manifest); err != nil {
		return errors.Trace(err)
	}
	h.nextSeq++
	h.buf = h.buf[:0]
	return nil
}

func tableRefFromKey(key string) dumpTableRef {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return dumpTableRef{db: key[:i], table: key[i+1:]}
		}
	}
	return dumpTableRef{table: key}
}

func newLockNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read failing is not recoverable; a timestamp nonce is a weak
		// fallback that still distinguishes lock owners.
		return fmt.Sprintf("fallback-%d", utils.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
