package lease

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Revocation is delivered to Config.OnRevoke when a live session loses its
// lease. The receiver must stop streaming to the fenced connection. Callbacks
// are invoked without holding the manager lock, but may be delivered from a
// different goroutine than the one owning the stream.
type Revocation struct {
	// ServerID is the replicated server id.
	ServerID uint32
	// InstanceID is the fenced instance.
	InstanceID string
	// Generation is the fencing token of the revoked session.
	Generation uint64
	// Reason is one of the RevocationReason* constants.
	Reason string
}

// Config configures a Manager.
type Config struct {
	// Store holds the persistent lease records. Required.
	Store Store

	// OnRevoke, if set, is notified when a live session is fenced by a
	// same-instance reconnect, a duplicate conflict, an explicit takeover or an
	// expunge. It is never called for an ordinary Session.Release.
	OnRevoke func(Revocation)

	// Clock is used for all timestamps. Defaults to time.Now.
	Clock func() time.Time
}

// liveSession is the in-memory binding of a generation to a network session.
type liveSession struct {
	instanceID string
	generation uint64
}

// Manager enforces replica leases for one server process. It is safe for
// concurrent use.
type Manager struct {
	store    Store
	onRevoke func(Revocation)
	clock    func() time.Time

	mu      sync.Mutex
	loaded  bool
	records map[uint32]*Record
	live    map[uint32]*liveSession
}

// NewManager creates a Manager. Load must be called before Admit.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidArgument)
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Manager{
		store:    cfg.Store,
		onRevoke: cfg.OnRevoke,
		clock:    clock,
		records:  make(map[uint32]*Record),
		live:     make(map[uint32]*liveSession),
	}, nil
}

// Load reads all persisted records. It must be called once at startup, before
// serving connections. Records left active by a previous process stay bound to
// their owner but have no live session, so only the same instance identity can
// resume them. Corrupt or invalid persisted state makes Load fail; the manager
// then remains unloaded and refuses admission.
func (m *Manager) Load(ctx context.Context) error {
	records, err := m.store.LoadAll(ctx)
	if err != nil {
		return err
	}

	byID := make(map[uint32]*Record, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}
		if r.ServerID == 0 {
			return fmt.Errorf("%w: persisted lease with zero server id", ErrInvalidArgument)
		}
		if r.OwnerInstanceID == "" {
			return fmt.Errorf("lease: persisted lease for server id %d has no owner", r.ServerID)
		}
		switch r.Status {
		case StatusActive, StatusQuarantined:
		case "":
			r.Status = StatusActive
		default:
			return fmt.Errorf("lease: persisted lease for server id %d has invalid status %q", r.ServerID, r.Status)
		}
		if r.Generation == 0 {
			return fmt.Errorf("lease: persisted lease for server id %d has zero generation", r.ServerID)
		}
		byID[r.ServerID] = cloneRecord(r)
	}

	m.mu.Lock()
	m.records = byID
	m.live = make(map[uint32]*liveSession)
	m.loaded = true
	m.mu.Unlock()
	return nil
}

// Session is the handle for one admitted connection. All methods fail with
// ErrSessionRevoked once the session is fenced by a newer generation.
type Session struct {
	mgr *Manager
	id  Identity
	gen uint64
}

// Identity returns the identity the session was admitted for.
func (s *Session) Identity() Identity { return s.id }

// Generation returns the fencing token of the session.
func (s *Session) Generation() uint64 { return s.gen }

// Admit checks whether the identified replica may start streaming from loc and
// either returns the live session or denies the claim (see package
// documentation for the decision table). A successful grant or resume records
// loc as the latest requested location and bumps the generation.
func (m *Manager) Admit(ctx context.Context, id Identity, loc Location) (*Session, error) {
	if err := id.validate(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	if !m.loaded {
		m.mu.Unlock()
		return nil, ErrNotLoaded
	}
	now := m.now()

	rec, exists := m.records[id.ServerID]
	if !exists {
		nr := &Record{
			ServerID:        id.ServerID,
			OwnerInstanceID: id.InstanceID,
			Generation:      1,
			Status:          StatusActive,
			LastLocation:    loc,
			CreatedAt:       now,
			LastSeenAt:      now,
		}
		if err := m.store.Save(ctx, nr); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("lease: persist grant for server id %d: %w", id.ServerID, err)
		}
		m.records[id.ServerID] = nr
		m.live[id.ServerID] = &liveSession{instanceID: id.InstanceID, generation: 1}
		m.mu.Unlock()
		return m.session(id, 1), nil
	}

	// A live session with a different owner is an undecidable conflict: fence
	// both sides and quarantine until an operator picks a winner.
	if live := m.live[id.ServerID]; live != nil && live.instanceID != id.InstanceID {
		nr := cloneRecord(rec)
		nr.Status = StatusQuarantined
		nr.Quarantine = &Quarantine{
			Reason: ReasonDuplicateConcurrent,
			Since:  now,
			Detail: fmt.Sprintf(
				"server id %d claimed concurrently by %q (generation %d) and %q",
				id.ServerID, live.instanceID, live.generation, id.InstanceID),
			Candidates: []Candidate{
				{InstanceID: live.instanceID, Location: rec.LastLocation, ObservedAt: now},
				{InstanceID: id.InstanceID, Location: loc, ObservedAt: now},
			},
		}
		nr.LastSeenAt = now
		if err := m.store.Save(ctx, nr); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("lease: persist quarantine for server id %d: %w", id.ServerID, err)
		}
		m.records[id.ServerID] = nr
		rev := m.fenceLocked(id.ServerID, ReasonDuplicateConcurrent)
		m.mu.Unlock()
		m.notify(rev)
		return nil, &AdmissionError{
			Reason:     ReasonDuplicateConcurrent,
			ServerID:   id.ServerID,
			InstanceID: id.InstanceID,
			Err:        ErrQuarantined,
		}
	}

	// No live contender, but the lease is already bound to another instance and
	// no explicit takeover happened: the identity is unknown, do not hand over.
	if rec.OwnerInstanceID != id.InstanceID {
		nr := cloneRecord(rec)
		reason := ReasonUnknownInstance
		// A quarantined lease keeps its original (duplicate) reason so the
		// evidence of the first conflict is not lost by later retry noise.
		if rec.Status == StatusQuarantined && nr.Quarantine != nil {
			reason = nr.Quarantine.Reason
			nr.Quarantine.addCandidate(id.InstanceID, loc, now)
		} else {
			nr.Status = StatusQuarantined
			nr.Quarantine = &Quarantine{
				Reason: reason,
				Since:  now,
				Detail: fmt.Sprintf(
					"server id %d claimed by %q but bound to %q; explicit takeover required",
					id.ServerID, id.InstanceID, rec.OwnerInstanceID),
				Candidates: []Candidate{
					{InstanceID: id.InstanceID, Location: loc, ObservedAt: now},
				},
			}
		}
		nr.LastSeenAt = now
		if err := m.store.Save(ctx, nr); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("lease: persist quarantine for server id %d: %w", id.ServerID, err)
		}
		m.records[id.ServerID] = nr
		m.mu.Unlock()
		return nil, &AdmissionError{
			Reason:     reason,
			ServerID:   id.ServerID,
			InstanceID: id.InstanceID,
			Err:        ErrQuarantined,
		}
	}

	// Same identity: either a fresh resume after disconnect, a reconnect while
	// the previous session is still live, or the owner returning to a lease
	// that was quarantined by an outsider. Identity is certain in every case,
	// so the lease is resumed and the previous generation is fenced.
	nr := cloneRecord(rec)
	nr.Status = StatusActive
	nr.Quarantine = nil
	nr.Generation = rec.Generation + 1
	nr.LastLocation = mergeLocation(rec.LastLocation, loc)
	nr.LastSeenAt = now
	if err := m.store.Save(ctx, nr); err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("lease: persist resume for server id %d: %w", id.ServerID, err)
	}
	m.records[id.ServerID] = nr

	var rev Revocation
	if live := m.live[id.ServerID]; live != nil {
		rev = m.fenceLocked(id.ServerID, RevocationReasonSuperseded)
	}
	m.live[id.ServerID] = &liveSession{instanceID: id.InstanceID, generation: nr.Generation}
	m.mu.Unlock()
	if rev.Reason != "" {
		m.notify(rev)
	}
	return m.session(id, nr.Generation), nil
}

// Takeover explicitly transfers the lease of serverID to byInstanceID. It is
// the only way a quarantined lease can change owner and also works on active
// leases when an operator forces a switch. The currently live session (if any)
// is fenced and the generation is bumped. The new owner must subsequently call
// Admit to obtain a session.
func (m *Manager) Takeover(ctx context.Context, serverID uint32, byInstanceID, reason string) error {
	if serverID == 0 {
		return fmt.Errorf("%w: server id must not be zero", ErrInvalidArgument)
	}
	if byInstanceID == "" {
		return fmt.Errorf("%w: new owner instance id is empty", ErrInvalidArgument)
	}

	m.mu.Lock()
	if !m.loaded {
		m.mu.Unlock()
		return ErrNotLoaded
	}
	rec, ok := m.records[serverID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: server id %d", ErrLeaseNotFound, serverID)
	}

	now := m.now()
	nr := cloneRecord(rec)
	previousOwner := rec.OwnerInstanceID
	if previousOwner == byInstanceID {
		previousOwner = ""
	}
	nr.OwnerInstanceID = byInstanceID
	nr.Generation = rec.Generation + 1
	nr.Status = StatusActive
	nr.Quarantine = nil
	nr.LastSeenAt = now
	nr.LastTakeover = &Takeover{
		By:             byInstanceID,
		Reason:         reason,
		At:             now,
		PreviousOwner:  previousOwner,
		FromGeneration: rec.Generation,
	}
	if err := m.store.Save(ctx, nr); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("lease: persist takeover for server id %d: %w", serverID, err)
	}
	m.records[serverID] = nr

	var rev Revocation
	if live := m.live[serverID]; live != nil {
		rev = m.fenceLocked(serverID, RevocationReasonTakeover)
	}
	m.mu.Unlock()
	if rev.Reason != "" {
		m.notify(rev)
	}
	return nil
}

// Expunge deletes a lease entirely, e.g. when a replica is decommissioned. A
// live session is fenced first.
func (m *Manager) Expunge(ctx context.Context, serverID uint32) error {
	m.mu.Lock()
	if !m.loaded {
		m.mu.Unlock()
		return ErrNotLoaded
	}
	if _, ok := m.records[serverID]; !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: server id %d", ErrLeaseNotFound, serverID)
	}
	if err := m.store.Delete(ctx, serverID); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("lease: delete server id %d: %w", serverID, err)
	}
	delete(m.records, serverID)
	var rev Revocation
	if live := m.live[serverID]; live != nil {
		rev = m.fenceLocked(serverID, RevocationReasonExpunged)
	}
	m.mu.Unlock()
	if rev.Reason != "" {
		m.notify(rev)
	}
	return nil
}

// Advance records that the session requested or acknowledged loc. Locations
// only move forward; an older location does not roll the lease back.
func (s *Session) Advance(ctx context.Context, loc Location) error {
	if loc.IsZero() {
		return fmt.Errorf("%w: location is empty", ErrInvalidArgument)
	}
	return s.mutate(ctx, func(rec *Record, now time.Time) {
		rec.LastLocation = mergeLocation(rec.LastLocation, loc)
		rec.LastSeenAt = now
	})
}

// Heartbeat refreshes the last-seen timestamp without changing the location.
func (s *Session) Heartbeat(ctx context.Context) error {
	return s.mutate(ctx, func(rec *Record, now time.Time) {
		rec.LastSeenAt = now
	})
}

// Release ends the session normally (connection closed). The persisted binding
// is retained so the same instance identity can resume later; only the live
// session is removed. Releasing an already superseded session is a no-op.
func (s *Session) Release(ctx context.Context) error {
	m := s.mgr
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.loaded {
		return ErrNotLoaded
	}

	rec, ok := s.currentLocked()
	if !ok {
		// A newer generation already owns the lease: nothing to release.
		return nil
	}

	nr := cloneRecord(rec)
	nr.LastSeenAt = m.now()
	if err := m.store.Save(ctx, nr); err != nil {
		return fmt.Errorf("lease: persist release for server id %d: %w", s.id.ServerID, err)
	}
	m.records[s.id.ServerID] = nr
	delete(m.live, s.id.ServerID)
	return nil
}

// mutate applies change to a cloned current record, persists it and commits it
// on success.
func (s *Session) mutate(ctx context.Context, change func(rec *Record, now time.Time)) error {
	m := s.mgr
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.loaded {
		return ErrNotLoaded
	}

	rec, ok := s.currentLocked()
	if !ok {
		return ErrSessionRevoked
	}

	nr := cloneRecord(rec)
	change(nr, m.now())
	if err := m.store.Save(ctx, nr); err != nil {
		return fmt.Errorf("lease: persist update for server id %d: %w", s.id.ServerID, err)
	}
	m.records[s.id.ServerID] = nr
	return nil
}

// currentLocked returns the record backing s. It returns false when the session
// has been fenced (newer generation, takeover, expunge).
func (s *Session) currentLocked() (*Record, bool) {
	m := s.mgr
	rec := m.records[s.id.ServerID]
	live := m.live[s.id.ServerID]
	if rec == nil || live == nil || live.generation != s.gen || live.instanceID != s.id.InstanceID {
		return nil, false
	}
	return rec, true
}

// Get returns a copy of the persisted record for serverID.
func (m *Manager) Get(serverID uint32) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[serverID]
	if !ok {
		return Record{}, false
	}
	return *cloneRecord(rec), true
}

// List returns copies of all persisted records ordered by server id.
func (m *Manager) List() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]Record, 0, len(m.records))
	for _, r := range m.records {
		out = append(out, *cloneRecord(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServerID < out[j].ServerID })
	return out
}

func (m *Manager) session(id Identity, generation uint64) *Session {
	return &Session{mgr: m, id: id, gen: generation}
}

// fenceLocked marks the current live session revoked and removes it. The
// returned revocation must be delivered after unlocking.
func (m *Manager) fenceLocked(serverID uint32, reason string) Revocation {
	live := m.live[serverID]
	if live == nil {
		return Revocation{}
	}
	delete(m.live, serverID)
	return Revocation{
		ServerID:   serverID,
		InstanceID: live.instanceID,
		Generation: live.generation,
		Reason:     reason,
	}
}

func (m *Manager) notify(rev Revocation) {
	if rev.Reason == "" || m.onRevoke == nil {
		return
	}
	m.onRevoke(rev)
}

func (m *Manager) now() time.Time {
	return m.clock()
}
