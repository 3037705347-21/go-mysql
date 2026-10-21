package lease

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testEnv struct {
	store *MemoryStore
	now   time.Time
	revs  []Revocation
	mu    sync.Mutex
}

func newTestEnv(t *testing.T) (*testEnv, *Manager) {
	t.Helper()
	env := &testEnv{
		store: NewMemoryStore(),
		now:   time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC),
	}
	mgr, err := NewManager(Config{
		Store: env.store,
		Clock: func() time.Time { return env.now },
		OnRevoke: func(r Revocation) {
			env.mu.Lock()
			defer env.mu.Unlock()
			env.revs = append(env.revs, r)
		},
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Load(context.Background()))
	return env, mgr
}

func (e *testEnv) advance(d time.Duration) { e.now = e.now.Add(d) }

func (e *testEnv) revocations() []Revocation {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Revocation, len(e.revs))
	copy(out, e.revs)
	return out
}

func loc(name string, pos uint32) Location {
	return Location{BinlogName: name, BinlogPos: pos}
}

func idA() Identity { return Identity{ServerID: 100, InstanceID: "replica-A"} }
func idB() Identity { return Identity{ServerID: 100, InstanceID: "replica-B"} }

func TestAdmit_FirstGrant(t *testing.T) {
	_, mgr := newTestEnv(t)

	s, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.Generation())
	require.Equal(t, idA(), s.Identity())

	rec, ok := mgr.Get(100)
	require.True(t, ok)
	require.Equal(t, StatusActive, rec.Status)
	require.Equal(t, "replica-A", rec.OwnerInstanceID)
	require.Equal(t, uint64(1), rec.Generation)
	require.Equal(t, loc("mysql-bin.000001", 4), rec.LastLocation)
	require.Nil(t, rec.Quarantine)
}

func TestAdmit_RejectsUnknownIdentity(t *testing.T) {
	_, mgr := newTestEnv(t)

	_, err := mgr.Admit(context.Background(), Identity{ServerID: 100}, loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrUnknownIdentity)

	_, err = mgr.Admit(context.Background(), Identity{ServerID: 0, InstanceID: "x"}, loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestAdmit_BeforeLoadIsRefused(t *testing.T) {
	mgr, err := NewManager(Config{Store: NewMemoryStore()})
	require.NoError(t, err)

	_, err = mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrNotLoaded)
}

func TestAdmit_SameInstanceResumeFencesOldSession(t *testing.T) {
	env, mgr := newTestEnv(t)

	s1, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	env.advance(time.Second)
	s2, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 120))
	require.NoError(t, err)
	require.Equal(t, uint64(2), s2.Generation())

	// the old session must be fenced
	require.ErrorIs(t, s1.Heartbeat(context.Background()), ErrSessionRevoked)

	revs := env.revocations()
	require.Len(t, revs, 1)
	require.Equal(t, RevocationReasonSuperseded, revs[0].Reason)
	require.Equal(t, uint64(1), revs[0].Generation)

	rec, ok := mgr.Get(100)
	require.True(t, ok)
	require.Equal(t, uint64(2), rec.Generation)
	require.Equal(t, StatusActive, rec.Status)
}

func TestAdmit_ConcurrentDuplicateQuarantinesBoth(t *testing.T) {
	env, mgr := newTestEnv(t)

	sA, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.NoError(t, sA.Advance(context.Background(), loc("mysql-bin.000002", 500)))

	env.advance(time.Second)
	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000003", 80))
	var admErr *AdmissionError
	require.ErrorAs(t, err, &admErr)
	require.ErrorIs(t, err, ErrQuarantined)
	require.Equal(t, ReasonDuplicateConcurrent, admErr.Reason)

	// owner session is fenced, no further streaming allowed
	require.ErrorIs(t, sA.Advance(context.Background(), loc("mysql-bin.000002", 600)), ErrSessionRevoked)

	revs := env.revocations()
	require.Len(t, revs, 1)
	require.Equal(t, ReasonDuplicateConcurrent, revs[0].Reason)
	require.Equal(t, "replica-A", revs[0].InstanceID)

	rec, ok := mgr.Get(100)
	require.True(t, ok)
	require.Equal(t, StatusQuarantined, rec.Status)
	require.NotNil(t, rec.Quarantine)
	require.Equal(t, ReasonDuplicateConcurrent, rec.Quarantine.Reason)
	require.Len(t, rec.Quarantine.Candidates, 2)
	require.Equal(t, loc("mysql-bin.000002", 500), rec.Quarantine.Candidates[0].Location)
	require.Equal(t, loc("mysql-bin.000003", 80), rec.Quarantine.Candidates[1].Location)
}

func TestQuarantined_RetriesAreRecordedButStillDenied(t *testing.T) {
	_, mgr := newTestEnv(t)

	sA, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrQuarantined)
	require.ErrorIs(t, sA.Heartbeat(context.Background()), ErrSessionRevoked)

	// outsider keeps retrying: evidence is updated, admission stays denied
	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000002", 99))
	require.ErrorIs(t, err, ErrQuarantined)

	rec, _ := mgr.Get(100)
	require.NotNil(t, rec.Quarantine)
	require.Len(t, rec.Quarantine.Candidates, 2) // A and B, not duplicated
	require.Equal(t, loc("mysql-bin.000002", 99), rec.Quarantine.Candidates[1].Location)
}

func TestQuarantined_OwnerCanResumeAndClear(t *testing.T) {
	env, mgr := newTestEnv(t)

	sA1, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrQuarantined)
	require.ErrorIs(t, sA1.Heartbeat(context.Background()), ErrSessionRevoked)

	env.advance(time.Second)
	sA2, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err, "bound owner must be allowed to resume a quarantined lease")
	require.Equal(t, uint64(2), sA2.Generation())

	rec, _ := mgr.Get(100)
	require.Equal(t, StatusActive, rec.Status)
	require.Nil(t, rec.Quarantine)
}

func TestAdmit_DetachedDifferentInstanceIsQuarantined(t *testing.T) {
	_, mgr := newTestEnv(t)

	sA, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.NoError(t, sA.Release(context.Background()))

	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000001", 4))
	var admErr *AdmissionError
	require.ErrorAs(t, err, &admErr)
	require.ErrorIs(t, err, ErrQuarantined)
	require.Equal(t, ReasonUnknownInstance, admErr.Reason)

	// the original owner can still resume without an explicit takeover
	sA2, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.Equal(t, uint64(2), sA2.Generation())
	rec, _ := mgr.Get(100)
	require.Equal(t, StatusActive, rec.Status)
	require.Nil(t, rec.Quarantine)
}

func TestTakeover_SwitchesOwner(t *testing.T) {
	env, mgr := newTestEnv(t)

	// A owns the lease, then goes away; B is rejected as an unknown instance.
	sA, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.NoError(t, sA.Advance(context.Background(), loc("mysql-bin.000002", 500)))
	require.NoError(t, sA.Release(context.Background()))

	_, err = mgr.Admit(context.Background(), idB(), loc("mysql-bin.000002", 500))
	require.ErrorIs(t, err, ErrQuarantined)

	// operator explicitly decides B continues
	env.advance(time.Second)
	require.NoError(t, mgr.Takeover(context.Background(), 100, "replica-B", "operator failover"))

	rec, _ := mgr.Get(100)
	require.Equal(t, StatusActive, rec.Status)
	require.Equal(t, "replica-B", rec.OwnerInstanceID)
	require.Nil(t, rec.Quarantine)
	require.Equal(t, uint64(2), rec.Generation)
	require.NotNil(t, rec.LastTakeover)
	require.Equal(t, "replica-B", rec.LastTakeover.By)
	require.Equal(t, "replica-A", rec.LastTakeover.PreviousOwner)
	require.Equal(t, uint64(1), rec.LastTakeover.FromGeneration)

	// B can now stream; the persisted last position is still available
	sB, err := mgr.Admit(context.Background(), idB(), loc("mysql-bin.000002", 500))
	require.NoError(t, err)
	require.Equal(t, uint64(3), sB.Generation())
	rec, _ = mgr.Get(100)
	require.Equal(t, loc("mysql-bin.000002", 500), rec.LastLocation)

	// A is now the outsider and must not sneak back in
	_, err = mgr.Admit(context.Background(), idA(), loc("mysql-bin.000002", 500))
	require.ErrorIs(t, err, ErrQuarantined)
}

func TestTakeover_FencesLiveSession(t *testing.T) {
	env, mgr := newTestEnv(t)

	sA, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	require.NoError(t, mgr.Takeover(context.Background(), 100, "replica-B", "forced"))
	require.ErrorIs(t, sA.Heartbeat(context.Background()), ErrSessionRevoked)

	revs := env.revocations()
	require.Len(t, revs, 1)
	require.Equal(t, RevocationReasonTakeover, revs[0].Reason)
	require.Equal(t, uint64(1), revs[0].Generation)

	// takeover does not invent a session: B still goes through Admit
	sB, err := mgr.Admit(context.Background(), idB(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.Equal(t, uint64(3), sB.Generation())
}

func TestTakeover_ValidatesInput(t *testing.T) {
	_, mgr := newTestEnv(t)

	require.ErrorIs(t, mgr.Takeover(context.Background(), 0, "b", ""), ErrInvalidArgument)
	require.ErrorIs(t, mgr.Takeover(context.Background(), 999, "b", ""), ErrLeaseNotFound)
}

func TestSession_AdvanceIsMonotonic(t *testing.T) {
	_, mgr := newTestEnv(t)
	s, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	require.NoError(t, s.Advance(context.Background(), loc("mysql-bin.000003", 10)))
	require.NoError(t, s.Advance(context.Background(), loc("mysql-bin.000002", 999))) // older file
	require.NoError(t, s.Advance(context.Background(), loc("mysql-bin.000003", 5)))   // older offset

	rec, _ := mgr.Get(100)
	require.Equal(t, loc("mysql-bin.000003", 10), rec.LastLocation)

	require.ErrorIs(t, s.Advance(context.Background(), Location{}), ErrInvalidArgument)
}

func TestSession_HeartbeatAndRelease(t *testing.T) {
	env, mgr := newTestEnv(t)
	s, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	env.advance(5 * time.Second)
	require.NoError(t, s.Heartbeat(context.Background()))
	rec, _ := mgr.Get(100)
	require.Equal(t, env.now, rec.LastSeenAt)

	env.advance(time.Second)
	require.NoError(t, s.Release(context.Background()))
	// release is idempotent and does not revoke
	require.NoError(t, s.Release(context.Background()))
	require.Empty(t, env.revocations())

	// lease persists for resume
	rec, _ = mgr.Get(100)
	require.Equal(t, StatusActive, rec.Status)
	require.Equal(t, "replica-A", rec.OwnerInstanceID)

	// heartbeat after release is a fencing error
	require.ErrorIs(t, s.Heartbeat(context.Background()), ErrSessionRevoked)
}

func TestExpunge_RemovesLeaseAndFences(t *testing.T) {
	env, mgr := newTestEnv(t)
	s, err := mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)

	require.NoError(t, mgr.Expunge(context.Background(), 100))
	require.ErrorIs(t, s.Heartbeat(context.Background()), ErrSessionRevoked)

	revs := env.revocations()
	require.Len(t, revs, 1)
	require.Equal(t, RevocationReasonExpunged, revs[0].Reason)

	_, ok := mgr.Get(100)
	require.False(t, ok)
	require.ErrorIs(t, mgr.Expunge(context.Background(), 100), ErrLeaseNotFound)

	// decommissioned server id can be granted fresh afterwards
	s2, err := mgr.Admit(context.Background(), idB(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.Equal(t, uint64(1), s2.Generation())
}

func TestRestartRecovery_PersistentLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.json")

	store1, err := NewFileStore(path)
	require.NoError(t, err)
	mgr1, err := NewManager(Config{Store: store1})
	require.NoError(t, err)
	require.NoError(t, mgr1.Load(context.Background()))

	sA, err := mgr1.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.NoError(t, err)
	require.NoError(t, sA.Advance(context.Background(), loc("mysql-bin.000005", 1234)))
	require.NoError(t, sA.Release(context.Background()))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	// simulate process restart: new manager, same backing file
	store2, err := NewFileStore(path)
	require.NoError(t, err)
	mgr2, err := NewManager(Config{Store: store2})
	require.NoError(t, err)
	require.NoError(t, mgr2.Load(context.Background()))

	rec, ok := mgr2.Get(100)
	require.True(t, ok)
	require.Equal(t, "replica-A", rec.OwnerInstanceID)
	require.Equal(t, loc("mysql-bin.000005", 1234), rec.LastLocation)

	// same identity resumes the old lease
	sA2, err := mgr2.Admit(context.Background(), idA(), loc("mysql-bin.000005", 1234))
	require.NoError(t, err)
	require.Equal(t, uint64(2), sA2.Generation())

	// different identity after restart is not trusted
	_, err = mgr2.Admit(context.Background(), idB(), loc("mysql-bin.000005", 1234))
	require.ErrorIs(t, err, ErrQuarantined)

	// takeover also survives the restart
	require.NoError(t, mgr2.Takeover(context.Background(), 100, "replica-B", "failover"))
	store3, err := NewFileStore(path)
	require.NoError(t, err)
	mgr3, err := NewManager(Config{Store: store3})
	require.NoError(t, err)
	require.NoError(t, mgr3.Load(context.Background()))
	rec, _ = mgr3.Get(100)
	require.Equal(t, "replica-B", rec.OwnerInstanceID)
	require.NotNil(t, rec.LastTakeover)
}

func TestLoad_CorruptFileFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))

	store, err := NewFileStore(path)
	require.NoError(t, err)
	mgr, err := NewManager(Config{Store: store})
	require.NoError(t, err)
	require.Error(t, mgr.Load(context.Background()))

	// manager stays unloaded: nothing is admitted
	_, err = mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrNotLoaded)
}

func TestLoad_InvalidRecordFailsClosed(t *testing.T) {
	store := NewMemoryStore()
	require.NoError(t, store.Save(context.Background(), &Record{
		ServerID:        100,
		OwnerInstanceID: "",
		Generation:      1,
		Status:          StatusActive,
	}))

	mgr, err := NewManager(Config{Store: store})
	require.NoError(t, err)
	require.Error(t, mgr.Load(context.Background()))
	_, err = mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.ErrorIs(t, err, ErrNotLoaded)
}

type failingStore struct {
	Store
}

func (failingStore) Save(context.Context, *Record) error {
	return errors.New("disk on fire")
}

func TestAdmit_PersistFailureFailsClosed(t *testing.T) {
	mgr, err := NewManager(Config{Store: failingStore{NewMemoryStore()}})
	require.NoError(t, err)
	require.NoError(t, mgr.Load(context.Background()))

	_, err = mgr.Admit(context.Background(), idA(), loc("mysql-bin.000001", 4))
	require.Error(t, err)

	// nothing was committed in memory
	_, ok := mgr.Get(100)
	require.False(t, ok)
	require.Empty(t, mgr.List())
}

func TestMergeLocation(t *testing.T) {
	cases := []struct {
		name string
		cur  Location
		next Location
		want Location
	}{
		{name: "empty current", cur: Location{}, next: loc("a.000001", 4), want: loc("a.000001", 4)},
		{name: "empty next", cur: loc("a.000001", 4), next: Location{}, want: loc("a.000001", 4)},
		{name: "newer file", cur: loc("a.000001", 900), next: loc("a.000002", 4), want: loc("a.000002", 4)},
		{name: "older file", cur: loc("a.000002", 4), next: loc("a.000001", 900), want: loc("a.000002", 4)},
		{name: "newer offset", cur: loc("a.000001", 4), next: loc("a.000001", 9), want: loc("a.000001", 9)},
		{
			name: "same point keeps gtid",
			cur:  loc("a.000001", 4),
			next: Location{BinlogName: "a.000001", BinlogPos: 4, GTIDSet: "uuid:1-5"},
			want: Location{BinlogName: "a.000001", BinlogPos: 4, GTIDSet: "uuid:1-5"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, mergeLocation(tc.cur, tc.next))
		})
	}
}
