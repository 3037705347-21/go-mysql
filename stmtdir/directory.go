// Package stmtdir implements a persistent, content-addressed directory of
// prepared statements for one logical database session.
//
// A SQL proxy keeps multiple prepared statements alive for the same logical
// session while the upstream physical connection may be replaced (failover,
// pool rotation) and the proxy process itself may restart. Physical statement
// handles are only valid on the connection that created them, so reusing a
// stale handle produces ER_UNKNOWN_STMT_HANDLER and an in-memory map cannot
// survive a restart.
//
// The directory solves this by binding each statement to a stable logical
// identity derived from the SQL text, the parameter-definition signature and
// the result-column signature:
//
//   - Declare records a logical statement and persists it, but never prepares
//     it upstream.
//   - Bind prepares the statement on the current physical connection on demand
//     and remembers that the resulting handle is only valid for the current
//     connection generation.
//   - InvalidatePhysical marks every physical handle stale after the upstream
//     connection is replaced; statements are re-prepared lazily on their next
//     use, so unused statements are never prepared.
//   - Execute additionally heals a stale handle discovered mid-flight
//     (ER_UNKNOWN_STMT_HANDLER) by rebinding once and retrying.
//   - The directory is persisted as a checksummed, atomically replaced file;
//     the previous complete directory is always retained as a backup. A
//     signature mismatch or corrupt directory makes execution fail instead of
//     silently using a wrong statement, and the last good directory is kept.
package stmtdir

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

var (
	// ErrNotFound is returned when a logical statement id is not in the
	// directory.
	ErrNotFound = errors.New("stmtdir: logical statement not found")
	// ErrSignatureChanged is returned when the physical backend prepares a
	// statement whose parameter or result signature differs from the declared
	// logical identity. Execution is refused and the directory is left
	// untouched.
	ErrSignatureChanged = errors.New("stmtdir: prepared statement signature changed")
	// ErrDirectoryCorrupt is returned when the persisted directory and its
	// backup fail verification. No file is overwritten and execution is
	// refused.
	ErrDirectoryCorrupt = errors.New("stmtdir: statement directory is corrupt")
	// ErrNoBackend is returned when a physical statement backend is required
	// but none is configured.
	ErrNoBackend = errors.New("stmtdir: no physical statement backend configured")
	// ErrClosed is returned when the directory has been closed.
	ErrClosed = errors.New("stmtdir: directory is closed")
)

// LogicalID is the stable, content-derived identity of a prepared statement.
// It does not change when the physical connection or process is replaced.
type LogicalID string

// String returns the identity as a hexadecimal string.
func (id LogicalID) String() string {
	return string(id)
}

// Entry is the persisted description of one logical statement. It contains no
// information about the current physical connection.
type Entry struct {
	ID              LogicalID
	SQL             string
	Params          int
	Columns         int
	ParamSignature  string
	ResultSignature string
	RawParamFields  [][]byte
	RawColumnFields [][]byte
	CreatedAt       time.Time
}

// Handle is the physical statement description returned by a Backend after a
// successful COM_STMT_PREPARE.
type Handle struct {
	// ID is the upstream statement handle, valid only on the connection that
	// created it.
	ID uint32
	// Params and Columns are the counts reported by the upstream server.
	Params  int
	Columns int
	// RawParamFields and RawColumnFields are the raw parameter and column
	// definition packets reported by the upstream server and are used to verify
	// the parameter and result signatures.
	RawParamFields  [][]byte
	RawColumnFields [][]byte
}

// Backend prepares and closes statements on one physical upstream connection.
// An implementation wraps e.g. *client.Conn; call NewStmtDirectoryBackend in
// the client package for the built-in adapter. Calls are made without the
// directory lock held.
type Backend interface {
	Prepare(ctx context.Context, query string) (Handle, error)
	Close(ctx context.Context, id uint32) error
}

// BackendFuncs adapts a pair of functions to the Backend interface.
type BackendFuncs struct {
	PrepareFunc func(ctx context.Context, query string) (Handle, error)
	CloseFunc   func(ctx context.Context, id uint32) error
}

// Prepare calls PrepareFunc.
func (b BackendFuncs) Prepare(ctx context.Context, query string) (Handle, error) {
	if b.PrepareFunc == nil {
		return Handle{}, ErrNoBackend
	}
	return b.PrepareFunc(ctx, query)
}

// Close calls CloseFunc. A missing CloseFunc treats close as best effort.
func (b BackendFuncs) Close(ctx context.Context, id uint32) error {
	if b.CloseFunc == nil {
		return nil
	}
	return b.CloseFunc(ctx, id)
}

// Stats describes the current directory state for observability and tests.
type Stats struct {
	// SessionID is the logical session owning the directory.
	SessionID string
	// Generation is incremented every time the physical connection changes.
	Generation uint64
	// Declared is the number of logical statements in the directory.
	Declared int
	// Bound is the number of statements currently prepared on the physical
	// connection of the current generation.
	Bound int
}

// errPrepareRestarted is delivered to waiters when the connection generation
// changed while their prepare was in flight. Callers loop and join the prepare
// for the new generation.
var errPrepareRestarted = errors.New("stmtdir: prepare restarted on a newer connection generation")

// prepareCall coalesces concurrent Bind calls: one goroutine prepares on the
// backend, the rest wait on done and share the result.
type prepareCall struct {
	generation uint64
	physicalID uint32
	err        error
	done       chan struct{}
}

// binding is the runtime state of one logical statement.
type binding struct {
	Entry

	// physicalID is the upstream handle; zero means not prepared on the
	// current connection.
	physicalID uint32
	// boundGeneration is the connection generation physicalID belongs to.
	boundGeneration uint64
	// conflict records an observed signature mismatch with the backend. The
	// next Bind re-prepares and re-verifies instead of trusting the cache.
	conflict bool
	// inflight is the prepare currently running for this statement;
	// concurrent callers wait on it instead of preparing again.
	inflight *prepareCall
}

// Directory is the persistent statement directory of one logical session.
// Instances are safe for concurrent use.
type Directory struct {
	sessionID string
	store     *fileStore
	backend   Backend
	now       func() time.Time

	mu         sync.Mutex
	entries    map[LogicalID]*binding
	generation uint64
	closed     bool

	// onWait is an optional hook invoked after a Bind call joins an in-flight
	// prepare. It exists for deterministic concurrency tests; nil in
	// production.
	onWait func()
}

// Option configures a Directory at open time.
type Option func(*config)

type config struct {
	backend     Backend
	primaryPath string
	now         func() time.Time
}

// WithBackend sets the physical statement backend. It may be nil for a
// persistence-only directory; Bind then fails with ErrNoBackend until
// SetBackend supplies one.
func WithBackend(backend Backend) Option {
	return func(c *config) { c.backend = backend }
}

// WithPrimaryPath overrides the default on-disk location
// (<dir>/<session-id>.stmtdir.json). The previous complete directory is kept
// next to it with a .bak suffix regardless of this setting.
func WithPrimaryPath(path string) Option {
	return func(c *config) { c.primaryPath = path }
}

// WithClock injects a clock, mainly for tests.
func WithClock(now func() time.Time) Option {
	return func(c *config) { c.now = now }
}

// Open loads the persistent statement directory for sessionID from dir,
// creating the directory and an empty statement directory if needed.
//
// If the primary file fails verification, the previous complete directory is
// restored from the backup. If both are corrupt or the session id stored in
// them does not match sessionID, ErrDirectoryCorrupt is returned, no file is
// modified and the returned directory is nil, so execution is impossible.
func Open(ctx context.Context, sessionID, dir string, opts ...Option) (*Directory, error) {
	cfg := config{now: func() time.Time { return time.Now().UTC() }}
	for _, opt := range opts {
		opt(&cfg)
	}

	primary := cfg.primaryPath
	if primary == "" {
		var err error
		primary, err = defaultPrimaryPath(dir, sessionID)
		if err != nil {
			return nil, err
		}
	}

	store := newFileStore(primary, primary+backupSuffix)
	payload, err := store.load(ctx)
	if err != nil {
		return nil, err
	}
	if payload != nil && payload.SessionID != sessionID {
		return nil, fmt.Errorf("%w: session id mismatch: %q != %q",
			ErrDirectoryCorrupt, payload.SessionID, sessionID)
	}

	d := &Directory{
		sessionID:  sessionID,
		store:      store,
		backend:    cfg.backend,
		now:        cfg.now,
		entries:    make(map[LogicalID]*binding),
		generation: 1,
	}
	if payload != nil {
		entries, err := payload.toBindings()
		if err != nil {
			return nil, err
		}
		d.entries = entries
	}
	return d, nil
}

// SetBackend replaces the physical statement backend. Callers that replace the
// physical connection itself must also call InvalidatePhysical; attaching a
// backend alone never invalidates cached handles.
func (d *Directory) SetBackend(backend Backend) {
	d.mu.Lock()
	d.backend = backend
	d.mu.Unlock()
}

// SessionID returns the logical session id the directory belongs to.
func (d *Directory) SessionID() string {
	return d.sessionID
}

// Stats returns a point-in-time snapshot of directory state.
func (d *Directory) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := Stats{
		SessionID:  d.sessionID,
		Generation: d.generation,
		Declared:   len(d.entries),
	}
	for _, b := range d.entries {
		if b.physicalID != 0 && b.boundGeneration == d.generation {
			s.Bound++
		}
	}
	return s
}

// Entries returns the declared statements ordered by logical id. The returned
// slice and byte fields are copies and may be modified by the caller.
func (d *Directory) Entries() []Entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Entry, 0, len(d.entries))
	for _, b := range d.entries {
		out = append(out, cloneEntry(b.Entry))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Entry returns one declared statement.
func (d *Directory) Entry(id LogicalID) (Entry, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.entries[id]
	if !ok {
		return Entry{}, false
	}
	return cloneEntry(b.Entry), true
}

// Declare records the logical identity of a statement, identified by its SQL
// text and the raw parameter and column definition packets (as produced by
// COM_STMT_PREPARE). Nothing is prepared upstream.
//
// Declaring the same SQL and signatures again is idempotent and returns the
// same LogicalID. The directory is persisted before Declare returns.
func (d *Directory) Declare(sql string, rawParamFields, rawColumnFields [][]byte) (LogicalID, error) {
	id, _, _ := computeIdentity(sql, rawParamFields, rawColumnFields)
	err := d.mutate(func(work map[LogicalID]*binding) error {
		if _, ok := work[id]; ok {
			return nil
		}
		work[id] = &binding{
			Entry: Entry{
				ID:              id,
				SQL:             sql,
				Params:          len(rawParamFields),
				Columns:         len(rawColumnFields),
				ParamSignature:  paramSigOf(rawParamFields),
				ResultSignature: resultSigOf(rawColumnFields),
				RawParamFields:  cloneBytesList(rawParamFields),
				RawColumnFields: cloneBytesList(rawColumnFields),
				CreatedAt:       d.now(),
			},
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// Bind returns a physical statement handle valid on the current connection
// generation, preparing the statement on demand if it has never been prepared
// or the physical connection was replaced since its last use.
//
// The parameter and result signatures reported by the backend are verified
// against the declared logical identity. On mismatch the freshly prepared
// handle is closed, ErrSignatureChanged is returned and the directory is not
// modified.
func (d *Directory) Bind(ctx context.Context, id LogicalID) (uint32, error) {
	// Bind performs network I/O without the directory lock. Concurrent callers
	// for the same statement share one in-flight prepare. The outer loop only
	// repeats when the physical connection changed while preparing.
	for {
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return 0, ErrClosed
		}
		b, ok := d.entries[id]
		if !ok {
			d.mu.Unlock()
			return 0, ErrNotFound
		}
		backend := d.backend
		if backend == nil {
			d.mu.Unlock()
			return 0, ErrNoBackend
		}
		generation := d.generation
		if !b.conflict && b.physicalID != 0 && b.boundGeneration == generation {
			physicalID := b.physicalID
			d.mu.Unlock()
			return physicalID, nil
		}
		if b.inflight != nil && b.inflight.generation == generation {
			call := b.inflight
			d.mu.Unlock()
			if d.onWait != nil {
				d.onWait()
			}
			select {
			case <-call.done:
				if errors.Is(call.err, errPrepareRestarted) {
					if err := ctx.Err(); err != nil {
						return 0, err
					}
					continue
				}
				return call.physicalID, call.err
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		call := &prepareCall{generation: generation, done: make(chan struct{})}
		b.inflight = call
		sql := b.SQL
		expectedID := b.ID
		expectedParamSig := b.ParamSignature
		expectedResultSig := b.ResultSignature
		expectedParams := b.Params
		expectedColumns := b.Columns
		d.mu.Unlock()

		handle, err := backend.Prepare(ctx, sql)
		if err != nil {
			return d.finishPrepare(id, call, 0, fmt.Errorf("stmtdir: prepare %s: %w", id, err))
		}

		gotID, gotParamSig, gotResultSig := computeIdentity(sql, handle.RawParamFields, handle.RawColumnFields)
		mismatch := gotParamSig != expectedParamSig || gotResultSig != expectedResultSig ||
			handle.Params != expectedParams || handle.Columns != expectedColumns || gotID != expectedID

		d.mu.Lock()
		current, exists := d.entries[id]
		switch {
		case !exists:
			// The statement was closed while Prepare was in flight.
			d.mu.Unlock()
			_ = backend.Close(ctx, handle.ID)
			return 0, ErrNotFound
		case d.generation != generation:
			// Connection swapped during Prepare; the new handle belongs to a
			// dead connection. InvalidatePhysical has usually already signaled
			// the waiters and cleared inflight; only signal again if it did
			// not see this call.
			if current.inflight == call {
				current.inflight = nil
				call.err = errPrepareRestarted
				close(call.done)
			}
			d.mu.Unlock()
			_ = backend.Close(ctx, handle.ID)
			continue
		case current.inflight != call:
			// Removed and re-declared while Prepare was in flight.
			d.mu.Unlock()
			_ = backend.Close(ctx, handle.ID)
			return 0, ErrNotFound
		case mismatch:
			// The upstream statement shape changed (e.g. a table was altered).
			// Refuse execution and release the handle instead of binding it.
			current.physicalID = 0
			current.boundGeneration = 0
			current.conflict = true
			current.inflight = nil
			rerr := fmt.Errorf("%w: statement %s now reports params=%d columns=%d (declared params=%d columns=%d)",
				ErrSignatureChanged, id, handle.Params, handle.Columns, expectedParams, expectedColumns)
			call.err = rerr
			close(call.done)
			d.mu.Unlock()
			_ = backend.Close(ctx, handle.ID)
			return 0, rerr
		default:
			current.physicalID = handle.ID
			current.boundGeneration = generation
			current.conflict = false
			current.inflight = nil
			call.physicalID = handle.ID
			close(call.done)
			d.mu.Unlock()
			return handle.ID, nil
		}
	}
}

// finishPrepare records the outcome of a failed backend prepare, wakes every
// caller waiting on it and returns the same result to the leader.
func (d *Directory) finishPrepare(id LogicalID, call *prepareCall, physicalID uint32, err error) (uint32, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if b, ok := d.entries[id]; ok && b.inflight == call {
		b.inflight = nil
		call.physicalID = physicalID
		call.err = err
		close(call.done)
	}
	return physicalID, err
}

// Execute binds the statement and runs exec with its physical handle. If exec
// reports ER_UNKNOWN_STMT_HANDLER because the connection was swapped before
// the generation was invalidated, the statement is rebound exactly once and
// exec retried. Other errors and signature mismatches are returned to the
// caller and execution is refused.
func (d *Directory) Execute(ctx context.Context, id LogicalID, exec func(physicalID uint32) error) error {
	if exec == nil {
		return errors.New("stmtdir: exec callback is nil")
	}
	physicalID, err := d.Bind(ctx, id)
	if err != nil {
		return err
	}
	if err := exec(physicalID); err != nil {
		if !IsUnknownStmtHandler(err) || !d.invalidateBinding(id, physicalID) {
			return err
		}
		newID, bindErr := d.Bind(ctx, id)
		if bindErr != nil {
			return bindErr
		}
		return exec(newID)
	}
	return nil
}

// InvalidatePhysical marks every physical handle stale after the upstream
// connection has been replaced. No statements are closed upstream (the old
// connection is gone) and nothing is re-prepared; the next Bind of each
// statement prepares it lazily on the new connection.
func (d *Directory) InvalidatePhysical() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.generation++
	for _, b := range d.entries {
		b.physicalID = 0
		b.boundGeneration = 0
		b.conflict = false
		if call := b.inflight; call != nil {
			// The handle being prepared belongs to the dying connection; make
			// waiters join the prepare for the new generation instead of
			// accepting it.
			b.inflight = nil
			call.err = errPrepareRestarted
			close(call.done)
		}
	}
}

// CloseStmt removes a logical statement from the directory, persists the
// change and, when the statement is prepared on the current connection, asks
// the backend to release the upstream handle. The directory mutation persists
// even if closing the upstream handle fails; that error is still returned.
func (d *Directory) CloseStmt(ctx context.Context, id LogicalID) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	removed, ok := d.entries[id]
	if !ok {
		d.mu.Unlock()
		return ErrNotFound
	}

	work := make(map[LogicalID]*binding, len(d.entries)-1)
	for k, b := range d.entries {
		if k != id {
			work[k] = b
		}
	}
	data, err := encodePayload(d.sessionID, work)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	if err := d.store.save(data); err != nil {
		d.mu.Unlock()
		return err
	}
	d.entries = work
	backend := d.backend
	generation := d.generation
	if call := removed.inflight; call != nil {
		removed.inflight = nil
		call.err = ErrNotFound
		close(call.done)
	}
	d.mu.Unlock()

	if removed.physicalID != 0 && removed.boundGeneration == generation && backend != nil {
		if err := backend.Close(ctx, removed.physicalID); err != nil {
			return fmt.Errorf("stmtdir: close upstream handle %d: %w", removed.physicalID, err)
		}
	}
	return nil
}

// Close marks the directory closed. Persistent state is already durable after
// each mutation, so Close performs no I/O.
func (d *Directory) Close() error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	return nil
}

// IsUnknownStmtHandler reports whether err is the MySQL
// ER_UNKNOWN_STMT_HANDLER error (1243), which indicates a stale physical
// statement handle.
func IsUnknownStmtHandler(err error) bool {
	var myErr *mysql.MyError
	if errors.As(err, &myErr) {
		return myErr.Code == mysql.ER_UNKNOWN_STMT_HANDLER
	}
	return false
}

func (d *Directory) invalidateBinding(id LogicalID, physicalID uint32) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.entries[id]
	if !ok || b.physicalID != physicalID || b.boundGeneration != d.generation {
		return false
	}
	b.physicalID = 0
	b.boundGeneration = 0
	return true
}

// mutate applies fn to a working copy of the entry map, persists the result
// and only commits the working copy when the on-disk directory was replaced
// successfully. A persistence failure therefore leaves both memory and disk
// at the previous complete directory.
func (d *Directory) mutate(fn func(work map[LogicalID]*binding) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	work := make(map[LogicalID]*binding, len(d.entries))
	maps.Copy(work, d.entries)
	if err := fn(work); err != nil {
		return err
	}
	data, err := encodePayload(d.sessionID, work)
	if err != nil {
		return err
	}
	if err := d.store.save(data); err != nil {
		return err
	}
	d.entries = work
	return nil
}

// computeIdentity derives the logical id and separate parameter/result
// signatures from the SQL text and raw definition packets. Every field is
// length-prefixed so that concatenation is unambiguous.
func computeIdentity(sql string, rawParamFields, rawColumnFields [][]byte) (id LogicalID, paramSig, resultSig string) {
	ps := signatureHash(rawParamFields)
	rs := signatureHash(rawColumnFields)
	paramSig = hex.EncodeToString(ps)
	resultSig = hex.EncodeToString(rs)

	idDigest := sha256.New()
	writeLenBytes(idDigest, []byte(sql))
	writeCount(idDigest, 2)
	idDigest.Write(ps)
	idDigest.Write(rs)
	id = LogicalID(hex.EncodeToString(idDigest.Sum(nil)))
	return id, paramSig, resultSig
}

// paramSigOf and resultSigOf hash one field list independently.
func paramSigOf(rawParamFields [][]byte) string {
	return hex.EncodeToString(signatureHash(rawParamFields))
}

func resultSigOf(rawColumnFields [][]byte) string {
	return hex.EncodeToString(signatureHash(rawColumnFields))
}

func signatureHash(fields [][]byte) []byte {
	h := sha256.New()
	writeFieldList(h, fields)
	return h.Sum(nil)
}

func writeFieldList(h hash.Hash, fields [][]byte) {
	writeCount(h, len(fields))
	for _, f := range fields {
		writeLenBytes(h, f)
	}
}

func writeCount(h hash.Hash, n int) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	h.Write(buf[:])
}

func writeLenBytes(h hash.Hash, b []byte) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(len(b)))
	h.Write(buf[:])
	h.Write(b)
}

func cloneEntry(e Entry) Entry {
	e.RawParamFields = cloneBytesList(e.RawParamFields)
	e.RawColumnFields = cloneBytesList(e.RawColumnFields)
	return e
}

func cloneBytesList(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i, b := range in {
		out[i] = append([]byte(nil), b...)
	}
	return out
}
