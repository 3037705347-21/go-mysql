package stmtdir

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

const (
	sqlSelect = "SELECT ? AS n"
	sqlInsert = "INSERT INTO t(a, b) VALUES (?, ?)"
)

type stmtSpec struct {
	params  [][]byte
	columns [][]byte
}

func paramFields(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("param-def-%d", i))
	}
	return out
}

func columnFields(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("result-def-%d", i))
	}
	return out
}

// fakeBackend simulates one physical upstream connection. Handles in live are
// valid server-side; swap wipes them as a real server cutover would.
type fakeBackend struct {
	mu         sync.Mutex
	specs      map[string]stmtSpec
	nextID     uint32
	live       map[uint32]bool
	closed     []uint32
	prepares   int
	prepareErr error
	alwaysBad  bool

	gate   chan struct{} // non-nil Prepare blocks until closed
	active atomic.Int32  // prepares currently blocked in gate
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		specs:  make(map[string]stmtSpec),
		live:   make(map[uint32]bool),
		nextID: 1,
	}
}

func (b *fakeBackend) set(sql string, spec stmtSpec) {
	b.mu.Lock()
	b.specs[sql] = spec
	b.mu.Unlock()
}

func (b *fakeBackend) Prepare(_ context.Context, query string) (Handle, error) {
	if b.gate != nil {
		b.active.Add(1)
		<-b.gate
		b.active.Add(-1)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepares++
	if b.prepareErr != nil {
		return Handle{}, b.prepareErr
	}
	spec, ok := b.specs[query]
	if !ok {
		return Handle{}, fmt.Errorf("fake backend: no statement spec for %q", query)
	}
	id := b.nextID
	b.nextID++
	b.live[id] = true
	return Handle{
		ID:              id,
		Params:          len(spec.params),
		Columns:         len(spec.columns),
		RawParamFields:  append([][]byte(nil), spec.params...),
		RawColumnFields: append([][]byte(nil), spec.columns...),
	}, nil
}

func (b *fakeBackend) Close(_ context.Context, id uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = append(b.closed, id)
	delete(b.live, id)
	return nil
}

func (b *fakeBackend) swap() {
	b.mu.Lock()
	b.live = map[uint32]bool{}
	b.mu.Unlock()
}

func (b *fakeBackend) kill(id uint32) {
	b.mu.Lock()
	delete(b.live, id)
	b.mu.Unlock()
}

// exec simulates COM_STMT_EXECUTE: dead handles report ER_UNKNOWN_STMT_HANDLER.
func (b *fakeBackend) exec(id uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.alwaysBad || !b.live[id] {
		return mysql.NewError(mysql.ER_UNKNOWN_STMT_HANDLER, "unknown handler")
	}
	return nil
}

func (b *fakeBackend) prepareCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepares
}

func (b *fakeBackend) liveCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.live)
}

func (b *fakeBackend) closedHandles() []uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint32(nil), b.closed...)
}

func openAt(t *testing.T, dir, sessionID string, backend Backend, opts ...Option) *Directory {
	t.Helper()
	d, err := Open(context.Background(), sessionID, dir, append([]Option{WithBackend(backend)}, opts...)...)
	require.NoError(t, err)
	return d
}

func TestDeclareIsLazyAndBindCachesHandle(t *testing.T) {
	b := newFakeBackend()
	b.set(sqlSelect, stmtSpec{params: paramFields(1), columns: columnFields(1)})
	b.set(sqlInsert, stmtSpec{params: paramFields(2)})
	d := openAt(t, t.TempDir(), "sess", b)

	id1, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)
	id2, err := d.Declare(sqlInsert, paramFields(2), nil)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	// Declaring must not touch the physical connection.
	require.Equal(t, 0, b.prepareCount())
	require.Equal(t, Stats{SessionID: "sess", Generation: 1, Declared: 2, Bound: 0}, d.Stats())

	pid1, err := d.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.Equal(t, 1, b.prepareCount())
	require.Equal(t, 1, b.liveCount())

	// Repeated Bind reuses the handle; the second statement stays unprepared.
	pid1Again, err := d.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.Equal(t, pid1, pid1Again)
	require.Equal(t, 1, b.prepareCount())

	_, err = d.Bind(context.Background(), id2)
	require.NoError(t, err)
	require.Equal(t, 2, b.prepareCount())
	require.Equal(t, 2, d.Stats().Bound)
}

func TestPhysicalSwapRebindsLazilyAndLeavesUnusedStatementsUnprepared(t *testing.T) {
	b := newFakeBackend()
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	insertSpec := stmtSpec{params: paramFields(2)}
	b.set(sqlSelect, selectSpec)
	b.set(sqlInsert, insertSpec)
	d := openAt(t, t.TempDir(), "sess", b)

	id1, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)
	id2, err := d.Declare(sqlInsert, insertSpec.params, nil)
	require.NoError(t, err)
	_, err = d.Bind(context.Background(), id1)
	require.NoError(t, err)
	_, err = d.Bind(context.Background(), id2)
	require.NoError(t, err)
	require.Equal(t, 2, b.prepareCount())

	// Upstream connection is replaced: every server-side handle is gone.
	b.swap()
	d.InvalidatePhysical()

	require.Equal(t, uint64(2), d.Stats().Generation)
	require.Equal(t, 0, d.Stats().Bound)
	require.Equal(t, 0, b.liveCount())
	require.Equal(t, 2, b.prepareCount(), "swap must not eagerly re-prepare anything")

	// Only the statement actually used is prepared on the new connection.
	pid1, err := d.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.Equal(t, 3, b.prepareCount())
	require.Equal(t, 1, b.liveCount())
	require.Equal(t, 1, d.Stats().Bound, "id2 is still unbound")

	_, err = d.Bind(context.Background(), id2)
	require.NoError(t, err)
	require.Equal(t, 4, b.prepareCount())
	require.Equal(t, 2, b.liveCount())
	require.NotEmpty(t, pid1)
}

func TestRestartRecoversDirectoryAndRebindsOnDemand(t *testing.T) {
	dir := t.TempDir()
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	insertSpec := stmtSpec{params: paramFields(2)}

	b1 := newFakeBackend()
	b1.set(sqlSelect, selectSpec)
	b1.set(sqlInsert, insertSpec)
	d1 := openAt(t, dir, "sess", b1)
	id1, err := d1.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)
	id2, err := d1.Declare(sqlInsert, insertSpec.params, nil)
	require.NoError(t, err)
	_, err = d1.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.NoError(t, d1.Close())

	// New process, new upstream connection: physical handles are meaningless.
	b2 := newFakeBackend()
	b2.set(sqlSelect, selectSpec)
	b2.set(sqlInsert, insertSpec)
	d2 := openAt(t, dir, "sess", b2)

	entries := d2.Entries()
	require.Len(t, entries, 2)
	require.ElementsMatch(t, []LogicalID{id1, id2},
		[]LogicalID{entries[0].ID, entries[1].ID})
	require.Equal(t, 0, d2.Stats().Bound, "physical bindings are not persisted")
	require.Equal(t, 0, b2.prepareCount())

	pid, err := d2.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.Equal(t, 1, b2.prepareCount())
	require.Equal(t, 1, b2.liveCount())
	require.NotZero(t, pid)

	_, ok := d2.Entry(id2)
	require.True(t, ok)
}

func TestSignatureChangeRefusesExecutionAndKeepsLastDirectory(t *testing.T) {
	dir := t.TempDir()
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	d := openAt(t, dir, "sess", b)

	id, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)
	oldPID, err := d.Bind(context.Background(), id)
	require.NoError(t, err)

	// Upstream schema changed after a connection swap: same SQL, two result
	// columns now instead of one.
	b.swap()
	d.InvalidatePhysical()
	b.set(sqlSelect, stmtSpec{params: paramFields(1), columns: columnFields(2)})

	_, err = d.Bind(context.Background(), id)
	require.ErrorIs(t, err, ErrSignatureChanged)
	require.Equal(t, 0, d.Stats().Bound, "mismatching handle is never bound")

	// The refused handle must be released upstream rather than leaked.
	closed := b.closedHandles()
	require.NotEmpty(t, closed)
	for _, c := range closed {
		require.NotEqual(t, oldPID, c)
	}

	// The conflict keeps being re-verified on every attempt; execution stays
	// refused while upstream disagrees with the directory.
	preparesBefore := b.prepareCount()
	_, err = d.Bind(context.Background(), id)
	require.ErrorIs(t, err, ErrSignatureChanged)
	require.Greater(t, b.prepareCount(), preparesBefore)

	// The persisted directory is untouched: its result signature still has
	// one column. A restart against a restored (original-schema) backend binds.
	entry, ok := d.Entry(id)
	require.True(t, ok)
	require.Equal(t, 1, entry.Columns)

	b2 := newFakeBackend()
	b2.set(sqlSelect, selectSpec)
	d2 := openAt(t, dir, "sess", b2)
	pid, err := d2.Bind(context.Background(), id)
	require.NoError(t, err)
	require.NotZero(t, pid)
}

func TestExecuteHealsUnknownHandlerWithOneRebind(t *testing.T) {
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	d := openAt(t, t.TempDir(), "sess", b)
	id, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)

	stalePID, err := d.Bind(context.Background(), id)
	require.NoError(t, err)

	// Upstream silently lost the handle but nobody called InvalidatePhysical.
	b.kill(stalePID)
	var calls int
	err = d.Execute(context.Background(), id, func(physicalID uint32) error {
		calls++
		return b.exec(physicalID)
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "exec retried exactly once after rebind")
	require.Equal(t, 2, b.prepareCount())

	// If every handle is unknown, only one rebind happens and the error
	// surfaces instead of looping forever.
	b.alwaysBad = true
	calls = 0
	err = d.Execute(context.Background(), id, func(physicalID uint32) error {
		calls++
		return b.exec(physicalID)
	})
	require.Error(t, err)
	require.True(t, IsUnknownStmtHandler(err))
	require.Equal(t, 2, calls)
}

func TestConcurrentBindPreparesOnce(t *testing.T) {
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	b.gate = make(chan struct{})
	d := openAt(t, t.TempDir(), "sess", b)
	id, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)

	const waiters = 49
	var wg sync.WaitGroup
	results := make(chan uint32, waiters+1)
	errs := make(chan error, waiters+1)

	// Leader parks inside Prepare on the gate.
	wg.Go(func() {
		pid, err := d.Bind(context.Background(), id)
		results <- pid
		errs <- err
	})
	require.Eventually(t, func() bool { return b.active.Load() == 1 },
		2_000_000_000, 1_000_000)

	// The rest join the same in-flight prepare instead of preparing again.
	for range waiters {
		wg.Go(func() {
			pid, err := d.Bind(context.Background(), id)
			results <- pid
			errs <- err
		})
	}
	close(b.gate)
	wg.Wait()
	close(results)
	close(errs)

	var first uint32
	n := 0
	for pid := range results {
		if n == 0 {
			first = pid
		}
		require.Equal(t, first, pid)
		n++
	}
	require.Equal(t, waiters+1, n)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, b.prepareCount())
}

func TestPrepareFailureIsBroadcastAndRetried(t *testing.T) {
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	boom := errors.New("upstream exploded")
	b.prepareErr = boom
	b.gate = make(chan struct{})
	d := openAt(t, t.TempDir(), "sess", b)
	id, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)

	// Deterministically park one leader in Prepare and four waiters on the
	// in-flight call before releasing the gate.
	var joined atomic.Int32
	d.onWait = func() { joined.Add(1) }

	const callers = 5
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			_, err := d.Bind(context.Background(), id)
			require.ErrorIs(t, err, boom)
		})
	}
	require.Eventually(t, func() bool { return b.active.Load() == 1 },
		2_000_000_000, 1_000_000)
	require.Eventually(t, func() bool { return joined.Load() == callers-1 },
		2_000_000_000, 1_000_000)

	close(b.gate)
	wg.Wait()
	require.Equal(t, 1, b.prepareCount(), "waiters share the failed prepare")

	// Recovery: the next attempt prepares fresh.
	b.mu.Lock()
	b.prepareErr = nil
	b.mu.Unlock()
	pid, err := d.Bind(context.Background(), id)
	require.NoError(t, err)
	require.NotZero(t, pid)
	require.Equal(t, 2, b.prepareCount())
}

func TestCloseStmtPersistsAndClosesUpstream(t *testing.T) {
	dir := t.TempDir()
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	insertSpec := stmtSpec{params: paramFields(2)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	b.set(sqlInsert, insertSpec)
	d := openAt(t, dir, "sess", b)
	id1, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)
	id2, err := d.Declare(sqlInsert, insertSpec.params, nil)
	require.NoError(t, err)

	// Removing an unbound statement persists without touching upstream.
	require.NoError(t, d.CloseStmt(context.Background(), id2))
	require.Empty(t, b.closedHandles())
	require.ErrorIs(t, d.CloseStmt(context.Background(), id2), ErrNotFound)

	pid, err := d.Bind(context.Background(), id1)
	require.NoError(t, err)
	require.NoError(t, d.CloseStmt(context.Background(), id1))
	require.Contains(t, b.closedHandles(), pid)

	d2 := openAt(t, dir, "sess", newFakeBackend())
	require.Empty(t, d2.Entries())
}

func TestDeclareIsIdempotentAndSignatureBindsIdentity(t *testing.T) {
	b := newFakeBackend()
	d := openAt(t, t.TempDir(), "sess", b)

	id1, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)
	id1Again, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)
	require.Equal(t, id1, id1Again)
	require.Len(t, d.Entries(), 1)

	// Same SQL, different result signature => a different logical statement.
	id2, err := d.Declare(sqlSelect, paramFields(1), columnFields(2))
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
	require.Len(t, d.Entries(), 2)
	require.Equal(t, 0, b.prepareCount())
}

func TestCorruptPrimaryRestoresPreviousCompleteDirectory(t *testing.T) {
	dir := t.TempDir()
	primary, backup := filepath.Join(dir, "sess.stmtdir.json"), filepath.Join(dir, "sess.stmtdir.json.bak")
	b := newFakeBackend()
	d := openAt(t, dir, "sess", b, WithPrimaryPath(primary))

	idA, err := d.Declare("SELECT 1", nil, columnFields(1))
	require.NoError(t, err)
	idB, err := d.Declare("SELECT 2", nil, columnFields(1))
	require.NoError(t, err)
	idC, err := d.Declare("SELECT 3", nil, columnFields(1))
	require.NoError(t, err)

	// After three saves the backup holds the previous complete directory
	// (A and B), the primary holds all three.
	require.FileExists(t, backup)
	require.NoError(t, os.WriteFile(primary, []byte("{not json"), 0o600))

	d2 := openAt(t, dir, "sess", b, WithPrimaryPath(primary))
	ids := []LogicalID{}
	for _, e := range d2.Entries() {
		ids = append(ids, e.ID)
	}
	require.ElementsMatch(t, []LogicalID{idA, idB}, ids)
	require.NotContains(t, ids, idC)

	// The restored primary verifies again.
	restored, err := os.ReadFile(primary)
	require.NoError(t, err)
	_, err = decodeEnvelope(restored)
	require.NoError(t, err)
}

func TestBothFilesCorruptRefuseAndKeepThem(t *testing.T) {
	dir := t.TempDir()
	primary := filepath.Join(dir, "sess.stmtdir.json")
	backup := primary + ".bak"
	b := newFakeBackend()
	d := openAt(t, dir, "sess", b, WithPrimaryPath(primary))
	_, err := d.Declare("SELECT 1", nil, columnFields(1))
	require.NoError(t, err)
	_, err = d.Declare("SELECT 2", nil, columnFields(1))
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(primary, []byte("broken-primary"), 0o600))
	require.NoError(t, os.WriteFile(backup, []byte("broken-backup"), 0o600))

	d2, err := Open(context.Background(), "sess", dir, WithBackend(b), WithPrimaryPath(primary))
	require.ErrorIs(t, err, ErrDirectoryCorrupt)
	require.Nil(t, d2)

	got, rerr := os.ReadFile(primary)
	require.NoError(t, rerr)
	require.Equal(t, "broken-primary", string(got))
	got, rerr = os.ReadFile(backup)
	require.NoError(t, rerr)
	require.Equal(t, "broken-backup", string(got), "last files are never overwritten on corruption")
}

func TestChecksumTamperingIsDetected(t *testing.T) {
	dir := t.TempDir()
	primary := filepath.Join(dir, "sess.stmtdir.json")
	b := newFakeBackend()
	d := openAt(t, dir, "sess", b, WithPrimaryPath(primary))
	id, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)

	good, err := os.ReadFile(primary)
	require.NoError(t, err)

	// Tamper with the content while leaving an (old) checksum in place.
	var env envelope
	require.NoError(t, json.Unmarshal(good, &env))
	content, err := base64.StdEncoding.DecodeString(env.Content)
	require.NoError(t, err)
	content[len(content)-1] ^= 0xff
	env.Content = base64.StdEncoding.EncodeToString(content)
	tampered, err := json.Marshal(&env)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(primary, tampered, 0o600))

	d2, err := Open(context.Background(), "sess", dir, WithBackend(b), WithPrimaryPath(primary))
	require.ErrorIs(t, err, ErrDirectoryCorrupt)
	require.Nil(t, d2)

	// A valid backup makes the directory usable again.
	require.NoError(t, os.WriteFile(primary+".bak", good, 0o600))
	d3 := openAt(t, dir, "sess", b, WithPrimaryPath(primary))
	_, ok := d3.Entry(id)
	require.True(t, ok)
}

func TestNoBackendRejectsBindButKeepsPersistence(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(context.Background(), "sess", dir)
	require.NoError(t, err)
	id, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)
	_, err = d.Bind(context.Background(), id)
	require.ErrorIs(t, err, ErrNoBackend)

	b := newFakeBackend()
	b.set(sqlSelect, stmtSpec{params: paramFields(1), columns: columnFields(1)})
	d.SetBackend(b)
	pid, err := d.Bind(context.Background(), id)
	require.NoError(t, err)
	require.NotZero(t, pid)
}

func TestSessionIDMismatchRejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	primary := filepath.Join(dir, "shared.stmtdir.json")
	b := newFakeBackend()
	d := openAt(t, dir, "sess-a", b, WithPrimaryPath(primary))
	_, err := d.Declare(sqlSelect, paramFields(1), columnFields(1))
	require.NoError(t, err)

	_, err = Open(context.Background(), "sess-b", dir, WithBackend(b), WithPrimaryPath(primary))
	require.ErrorIs(t, err, ErrDirectoryCorrupt)
}

func TestInvalidateDuringInFlightPrepareRestartsWaiters(t *testing.T) {
	selectSpec := stmtSpec{params: paramFields(1), columns: columnFields(1)}
	b := newFakeBackend()
	b.set(sqlSelect, selectSpec)
	b.gate = make(chan struct{})
	d := openAt(t, t.TempDir(), "sess", b)
	id, err := d.Declare(sqlSelect, selectSpec.params, selectSpec.columns)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			pid, err := d.Bind(context.Background(), id)
			require.NoError(t, err)
			require.NotZero(t, pid)
		})
	}
	require.Eventually(t, func() bool { return b.active.Load() == 1 },
		2_000_000_000, 1_000_000)

	// Connection cutover while the first prepare is still running. Its handle
	// must be discarded; everybody binds on generation 2.
	d.InvalidatePhysical()
	close(b.gate)
	wg.Wait()

	require.Equal(t, uint64(2), d.Stats().Generation)
	require.Equal(t, 1, d.Stats().Bound)
	require.Equal(t, 2, b.prepareCount())
}

func TestIsUnknownStmtHandler(t *testing.T) {
	require.True(t, IsUnknownStmtHandler(mysql.NewError(mysql.ER_UNKNOWN_STMT_HANDLER, "x")))
	require.False(t, IsUnknownStmtHandler(mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "x")))
	require.False(t, IsUnknownStmtHandler(errors.New("other")))
}
