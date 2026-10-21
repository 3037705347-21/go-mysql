package replication

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// memSourceStore is a deterministic SourceStore for policy tests.
type memSourceStore struct {
	mu      sync.Mutex
	rec     *SourceAttribution
	saves   int
	loadErr error
	saveErr error
}

func (s *memSourceStore) Load() (*SourceAttribution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	if s.rec == nil {
		return nil, nil
	}
	cp := *s.rec
	return &cp, nil
}

func (s *memSourceStore) Save(rec *SourceAttribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	cp := *rec
	s.rec = &cp
	s.saves++
	return nil
}

func (s *memSourceStore) get() *SourceAttribution {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil {
		return nil
	}
	cp := *s.rec
	return &cp
}

func newTestRoster(t *testing.T) (*SourceRoster, *memSourceStore) {
	t.Helper()
	store := &memSourceStore{}
	r, err := NewSourceRoster(store, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	require.NoError(t, err)
	r.SetFlushInterval(0) // every checkpoint is visible immediately
	return r, store
}

func mysqlIdentity(uuid string) SourceIdentity {
	return SourceIdentity{Flavor: mysql.MySQLFlavor, ServerUUID: uuid, Addr: "10.0.0.1:3306"}
}

const (
	uuidA = "aaaaaaaa-1111-1111-1111-111111111111"
	uuidB = "bbbbbbbb-2222-2222-2222-222222222222"
)

func TestSourceIdentityValid(t *testing.T) {
	require.True(t, mysqlIdentity(uuidA).Valid())
	require.False(t, mysqlIdentity("not-a-uuid").Valid())
	require.False(t, mysqlIdentity("").Valid())
	require.True(t, SourceIdentity{Flavor: mysql.MySQLFlavor, ServerUUID: "  " + uuidA + "  "}.Valid())

	require.True(t, SourceIdentity{Flavor: mysql.MariaDBFlavor, DomainID: 0, ServerID: 42}.Valid())
	require.False(t, SourceIdentity{Flavor: mysql.MariaDBFlavor, ServerID: 0}.Valid())
}

func TestSourceIdentityEqualIgnoresAddrAndCase(t *testing.T) {
	require.True(t, mysqlIdentity(uuidA).Equal(mysqlIdentity(uuidA)))
	a1 := mysqlIdentity(uuidA)
	a2 := mysqlIdentity("AAAAAAAA-1111-1111-1111-111111111111")
	a2.Addr = "10.9.9.9:3306" // same instance reached through a different address
	require.True(t, a1.Equal(a2))
	require.False(t, mysqlIdentity(uuidA).Equal(mysqlIdentity(uuidB)))
	require.False(t, mysqlIdentity(uuidA).Equal(SourceIdentity{Flavor: mysql.MariaDBFlavor, ServerID: 1}))

	m1 := SourceIdentity{Flavor: mysql.MariaDBFlavor, DomainID: 3, ServerID: 100}
	m2 := SourceIdentity{Flavor: mysql.MariaDBFlavor, DomainID: 3, ServerID: 100, Addr: "other:3306"}
	require.True(t, m1.Equal(m2))
	m3 := m2
	m3.ServerID = 101
	require.False(t, m1.Equal(m3))
}

func TestSourceRosterAdoptsFirstSource(t *testing.T) {
	r, store := newTestRoster(t)

	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))

	rec := r.Attribution()
	require.Equal(t, SourceStateTrusted, rec.State)
	require.Equal(t, uuidA, rec.Current.ServerUUID)
	require.False(t, rec.BeganAt.IsZero())
	require.Equal(t, mysql.MySQLFlavor, rec.Current.Flavor)

	// Adoption is persisted immediately.
	stored := store.get()
	require.NotNil(t, stored)
	require.Equal(t, SourceStateTrusted, stored.State)
	require.Equal(t, sourceRosterVersion, stored.Version)
}

func TestSourceRosterSameSourceReconnectAllowed(t *testing.T) {
	r, _ := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))

	// Repeated connections of the same identity are a normal disconnect retry:
	// they succeed and leave the tenure untouched.
	began := r.Attribution().BeganAt
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	rec := r.Attribution()
	require.Equal(t, SourceStateTrusted, rec.State)
	require.Equal(t, began, rec.BeganAt)
	require.Nil(t, rec.Pending)
}

func TestSourceRosterQuarantinesUnknownSource(t *testing.T) {
	r, store := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))

	err := r.Verify(mysqlIdentity(uuidB))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUnknownSource), "expected ErrUnknownSource, got %v", err)

	rec := r.Attribution()
	require.Equal(t, SourceStateQuarantined, rec.State)
	require.Equal(t, uuidA, rec.Current.ServerUUID, "trusted identity must be preserved while quarantined")
	require.NotNil(t, rec.Pending)
	require.Equal(t, uuidB, rec.Pending.Identity.ServerUUID)
	require.Equal(t, QuarantineReasonUnknownSource, rec.Pending.Reason)

	// The quarantine is durable: reloading the store shows the same state.
	stored := store.get()
	require.Equal(t, SourceStateQuarantined, stored.State)
	require.NotNil(t, stored.Pending)
	require.Equal(t, uuidB, stored.Pending.Identity.ServerUUID)

	// The stranger is still rejected on subsequent attempts...
	err = r.Verify(mysqlIdentity(uuidB))
	require.ErrorIs(t, err, ErrUnknownSource)
	// ...and so is the trusted instance until takeover is explicit.
	err = r.Verify(mysqlIdentity(uuidA))
	require.ErrorIs(t, err, ErrUnknownSource)
	require.Equal(t, SourceStateQuarantined, r.State())
}

func TestSourceRosterQuarantineSurvivesReload(t *testing.T) {
	store := &memSourceStore{}
	logger := slog.Default()
	r1, err := NewSourceRoster(store, logger)
	require.NoError(t, err)
	require.NoError(t, r1.Verify(mysqlIdentity(uuidA)))
	require.ErrorIs(t, r1.Verify(mysqlIdentity(uuidB)), ErrUnknownSource)

	// A fresh process loads the same store and refuses the stranger without
	// any in-memory hint.
	r2, err := NewSourceRoster(store, logger)
	require.NoError(t, err)
	require.Equal(t, SourceStateQuarantined, r2.State())
	require.ErrorIs(t, r2.Verify(mysqlIdentity(uuidB)), ErrUnknownSource)
	require.ErrorIs(t, r2.Verify(mysqlIdentity(uuidA)), ErrUnknownSource)
}

func TestSourceRosterTakeoverArchivesTenure(t *testing.T) {
	r, _ := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	r.Checkpoint(uuidA + ":1-10")
	require.ErrorIs(t, r.Verify(mysqlIdentity(uuidB)), ErrUnknownSource)

	// Explicit takeover trusts the parked instance and records the chain.
	require.NoError(t, r.TakeoverPending(uuidB+":1-3"))

	rec := r.Attribution()
	require.Equal(t, SourceStateTrusted, rec.State)
	require.Equal(t, uuidB, rec.Current.ServerUUID)
	require.Nil(t, rec.Pending)
	require.Len(t, rec.History, 1)
	require.Equal(t, uuidA, rec.History[0].Identity.ServerUUID)
	require.Equal(t, uuidA+":1-10", rec.History[0].GTIDSet)
	require.Equal(t, QuarantineReasonUnknownSource, rec.History[0].EndReason)
	require.Equal(t, uuidB+":1-3", rec.GTIDSet)

	// New instance now feeds normally.
	require.NoError(t, r.Verify(mysqlIdentity(uuidB)))
}

func TestSourceRosterTakeoverPendingWithoutQuarantine(t *testing.T) {
	r, _ := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	require.ErrorIs(t, r.TakeoverPending(""), ErrNoQuarantinedSource)
}

func TestSourceRosterTakeoverRejectsInvalidIdentity(t *testing.T) {
	r, _ := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	require.ErrorIs(t, r.Takeover(SourceIdentity{Flavor: mysql.MySQLFlavor}, ""), ErrInvalidSourceIdentity)
}

func TestSourceRosterTakeoverSameIdentityKeepsTenure(t *testing.T) {
	r, store := newTestRoster(t)
	// First contact cannot be identified: quarantine as undetermined.
	require.ErrorIs(t, r.Verify(SourceIdentity{Flavor: mysql.MySQLFlavor, Addr: "h:3306"}), ErrSourceUndetermined)
	require.Equal(t, QuarantineReasonUndetermined, r.Attribution().Pending.Reason)
	require.Equal(t, QuarantineReasonUndetermined, store.get().Pending.Reason)

	// Operator confirms the familiar-looking instance explicitly; no tenure
	// should be fabricated for the empty pre-quarantine identity.
	require.NoError(t, r.Takeover(mysqlIdentity(uuidA), ""))
	rec := r.Attribution()
	require.Equal(t, SourceStateTrusted, rec.State)
	require.Equal(t, uuidA, rec.Current.ServerUUID)
	require.Empty(t, rec.History)
}

func TestSourceRosterUndeterminedQuarantinesTrustedSource(t *testing.T) {
	r, _ := newTestRoster(t)
	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))

	err := r.Verify(SourceIdentity{Flavor: mysql.MySQLFlavor, Addr: "h:3306"})
	require.ErrorIs(t, err, ErrSourceUndetermined)
	rec := r.Attribution()
	require.Equal(t, SourceStateQuarantined, rec.State)
	require.Equal(t, QuarantineReasonUndetermined, rec.Pending.Reason)
	require.Equal(t, uuidA, rec.Current.ServerUUID)

	// Even a valid observation after an undetermined quarantine needs takeover.
	require.ErrorIs(t, r.Verify(mysqlIdentity(uuidA)), ErrUnknownSource)
}

func TestSourceRosterCheckpointOnlyTrustedAndRateLimited(t *testing.T) {
	store := &memSourceStore{}
	r, err := NewSourceRoster(store, slog.Default())
	require.NoError(t, err)
	r.SetFlushInterval(time.Hour) // only adoption flush expected below

	require.NoError(t, r.Verify(mysqlIdentity(uuidA)))
	savesAfterAdopt := store.saves

	r.Checkpoint(uuidA + ":1-1")
	require.Equal(t, uuidA+":1-1", r.Attribution().GTIDSet, "memory always tracks the high-water set")
	require.Equal(t, savesAfterAdopt, store.saves, "checkpoint within flush interval must not rewrite the file")

	require.NoError(t, r.Flush())
	require.Equal(t, savesAfterAdopt+1, store.saves)
	require.Equal(t, uuidA+":1-1", store.get().GTIDSet)

	// While quarantined, checkpoints must not refresh attribution.
	require.ErrorIs(t, r.Verify(mysqlIdentity(uuidB)), ErrUnknownSource)
	r.Checkpoint(uuidB + ":9-9")
	require.NotContains(t, r.Attribution().GTIDSet, uuidB)
}

func TestSourceRosterNewRejectsNilStore(t *testing.T) {
	_, err := NewSourceRoster(nil, slog.Default())
	require.Error(t, err)
}

func TestSourceRosterRejectsNewerVersion(t *testing.T) {
	store := &memSourceStore{rec: &SourceAttribution{Version: sourceRosterVersion + 1}}
	_, err := NewSourceRoster(store, slog.Default())
	require.Error(t, err)
}

func TestFileSourceStoreMissingLoadsNil(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roster.json")
	store := NewFileSourceStore(path)

	rec, err := store.Load()
	require.NoError(t, err)
	require.Nil(t, rec)

	in := &SourceAttribution{
		Version:   sourceRosterVersion,
		State:     SourceStateTrusted,
		Current:   mysqlIdentity(uuidA),
		GTIDSet:   uuidA + ":1-5",
		BeganAt:   time.Now().Add(-time.Hour).UTC().Truncate(time.Second),
		LastSeen:  time.Now().UTC().Truncate(time.Second),
		UpdatedAt: time.Now().UTC().Truncate(time.Second),
	}
	require.NoError(t, store.Save(in))
	require.FileExists(t, path)

	out, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, in.State, out.State)
	require.Equal(t, in.Current.ServerUUID, out.Current.ServerUUID)
	require.Equal(t, in.GTIDSet, out.GTIDSet)

	// File mode is owner-only (it sits next to replication credentials).
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(0o600), info.Mode().Perm())
}

func TestFileSourceStoreRejectsCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roster.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))
	_, err := NewFileSourceStore(path).Load()
	require.Error(t, err)
}

// TestFileSourceRosterEndToEnd exercises OpenSourceRoster against a real file,
// proving a reopened process observes quarantine and takeover decisions.
func TestFileSourceRosterEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "roster.json")

	r1, err := OpenSourceRoster(path, slog.Default())
	require.NoError(t, err)
	require.NoError(t, r1.Verify(mysqlIdentity(uuidA)))
	require.ErrorIs(t, r1.Verify(mysqlIdentity(uuidB)), ErrUnknownSource)

	r2, err := OpenSourceRoster(path, slog.Default())
	require.NoError(t, err)
	require.Equal(t, SourceStateQuarantined, r2.State())
	require.NoError(t, r2.TakeoverPending(""))
	require.Equal(t, SourceStateTrusted, r2.State())

	r3, err := OpenSourceRoster(path, slog.Default())
	require.NoError(t, err)
	require.Equal(t, SourceStateTrusted, r3.State())
	require.Equal(t, uuidB, r3.Attribution().Current.ServerUUID)
	require.Len(t, r3.Attribution().History, 1)
}
