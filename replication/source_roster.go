package replication

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
)

// Roster persistence format version. Bump when SourceAttribution changes in a
// way that older code cannot safely read.
const sourceRosterVersion = 1

// DefaultSourceRosterFlushInterval caps how often the high-water GTID set in
// the roster file is rewritten. Identity transitions (adopt / quarantine /
// takeover) are always flushed immediately.
const DefaultSourceRosterFlushInterval = time.Second

var (
	// ErrUnknownSource is returned when a connected instance's identity does
	// not match the trusted source on record. Sync is quarantined; events from
	// the stranger are never forwarded until an explicit Takeover.
	ErrUnknownSource = errors.New("unknown GTID source, sync quarantined pending explicit takeover")

	// ErrSourceUndetermined is returned when the instance identity cannot be
	// established (for example server_uuid cannot be read). Continuing blindly
	// is exactly how a silent primary swap gets merged, so sync is quarantined.
	ErrSourceUndetermined = errors.New("GTID source identity could not be determined, sync quarantined")

	// ErrNoQuarantinedSource is returned by TakeoverPending when no unknown
	// source is currently held in quarantine.
	ErrNoQuarantinedSource = errors.New("no quarantined GTID source to take over")

	// ErrInvalidSourceIdentity is returned when an explicit takeover tries to
	// trust an identity that is empty or malformed.
	ErrInvalidSourceIdentity = errors.New("invalid GTID source identity")
)

// SourceState is the gating state of the GTID sync source.
type SourceState string

const (
	// SourceStateTrusted means the connected instance's identity matches the
	// one on record and sync may continue.
	SourceStateTrusted SourceState = "trusted"
	// SourceStateQuarantined means the connected instance changed identity (or
	// the identity could not be determined). Sync is stopped and the suspect
	// identity is parked in Pending until an explicit Takeover.
	SourceStateQuarantined SourceState = "quarantined"
)

// Quarantine / handoff reasons recorded on PendingSource and SourceTenure.
const (
	QuarantineReasonUnknownSource  = "unknown_source"
	QuarantineReasonUndetermined   = "undetermined"
	QuarantineReasonManualTakeover = "manual_takeover"
)

// SourceIdentity identifies the MySQL/MariaDB instance feeding a GTID stream.
//
// For MySQL the authoritative identity is ServerUUID (it is also the SID
// prefix of every GTID the instance originates). MariaDB has no UUID in its
// GTID design, so DomainID/ServerID are used instead; ServerUUID is recorded
// when the server happens to expose one.
//
// Addr is recorded purely for forensics: the whole reason this roster exists
// is that the same client address (VIP, proxy, service name) can hide a
// different instance after a switchover, so Addr never participates in
// identity comparison.
type SourceIdentity struct {
	Flavor     string `json:"flavor"`
	ServerUUID string `json:"server_uuid,omitempty"`
	DomainID   uint32 `json:"domain_id,omitempty"`
	ServerID   uint32 `json:"server_id,omitempty"`
	Addr       string `json:"addr,omitempty"`
}

// Valid reports whether the identity is strong enough to be trusted.
func (id SourceIdentity) Valid() bool {
	flavor := id.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	switch flavor {
	case mysql.MySQLFlavor:
		_, err := uuid.Parse(strings.TrimSpace(id.ServerUUID))
		return err == nil
	case mysql.MariaDBFlavor:
		return id.ServerID != 0
	default:
		return false
	}
}

// Equal compares the security-relevant parts of two identities. Flavor and
// Addr differences (or empty Addr) never make identities unequal beyond the
// flavor mismatch itself.
func (id SourceIdentity) Equal(other SourceIdentity) bool {
	flavor := id.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	otherFlavor := other.Flavor
	if otherFlavor == "" {
		otherFlavor = mysql.MySQLFlavor
	}
	if flavor != otherFlavor {
		return false
	}
	switch flavor {
	case mysql.MySQLFlavor:
		return strings.EqualFold(strings.TrimSpace(id.ServerUUID), strings.TrimSpace(other.ServerUUID))
	case mysql.MariaDBFlavor:
		if id.DomainID != other.DomainID || id.ServerID != other.ServerID {
			return false
		}
		// Only compare UUIDs when both sides expose one; older MariaDB has none.
		if id.ServerUUID != "" && other.ServerUUID != "" {
			return strings.EqualFold(id.ServerUUID, other.ServerUUID)
		}
		return true
	default:
		return false
	}
}

func (id SourceIdentity) normalized() SourceIdentity {
	out := id
	out.ServerUUID = strings.ToLower(strings.TrimSpace(id.ServerUUID))
	if out.Flavor == "" {
		out.Flavor = mysql.MySQLFlavor
	}
	return out
}

// SourceTenure is a closed period during which one trusted instance fed the
// stream. Tenures form the provenance chain explaining how GTID history from
// several physical instances ended up in one checkpoint.
type SourceTenure struct {
	Identity  SourceIdentity `json:"identity"`
	GTIDSet   string         `json:"gtid_set,omitempty"`
	BeganAt   time.Time      `json:"began_at"`
	EndedAt   time.Time      `json:"ended_at"`
	EndReason string         `json:"end_reason,omitempty"`
}

// PendingSource is the suspect identity parked while sync is quarantined.
type PendingSource struct {
	Identity   SourceIdentity `json:"identity"`
	DetectedAt time.Time      `json:"detected_at"`
	Reason     string         `json:"reason"`
}

// SourceAttribution is the persisted provenance record for one GTID sync
// channel.
type SourceAttribution struct {
	// Version of the record format.
	Version int `json:"version"`

	// State is trusted or quarantined.
	State SourceState `json:"state"`

	// Current is the identity of the trusted instance (zero value while the
	// very first connection is still undetermined and quarantined).
	Current SourceIdentity `json:"current"`

	// GTIDSet is the high-water GTID set attributed to Current, i.e. the last
	// checkpoint seen while Current was trusted.
	GTIDSet string `json:"gtid_set,omitempty"`

	// BeganAt is when Current was first adopted.
	BeganAt time.Time `json:"began_at"`
	// LastSeen is the last trusted checkpoint / connection time.
	LastSeen time.Time `json:"last_seen"`

	// History holds previous trusted instances, newest entry last, so the
	// chain of takeovers survives process restarts.
	History []SourceTenure `json:"history,omitempty"`

	// Pending is the unknown / undetermined identity that caused quarantine.
	Pending *PendingSource `json:"pending,omitempty"`

	UpdatedAt time.Time `json:"updated_at"`
}

// SourceStore persists the SourceAttribution record. Implementations must be
// safe for concurrent use; the roster serializes calls but tests may share a
// store.
type SourceStore interface {
	// Load returns the stored record. A missing record must return
	// (nil, nil).
	Load() (*SourceAttribution, error)
	// Save atomically replaces the stored record.
	Save(rec *SourceAttribution) error
}

// SourceRoster keeps the durable source-attribution record and decides whether
// a connected GTID source may continue feeding the stream.
//
// Decision model:
//
//   - first connection to an empty roster: the instance is adopted as trusted
//     and the record is persisted immediately;
//   - reconnect to the same identity: allowed with no extra side effects, so
//     ordinary disconnect/retry loops behave exactly as before;
//   - different identity (primary swap behind a stable address): the stream is
//     quarantined, the suspect identity is persisted in Pending and sync is
//     stopped until Takeover is called explicitly;
//   - identity that cannot be determined: quarantined the same way.
type SourceRoster struct {
	mu            sync.Mutex
	store         SourceStore
	logger        *slog.Logger
	rec           *SourceAttribution
	flushInterval time.Duration
	lastFlush     time.Time
}

// NewSourceRoster loads the attribution record from store. A missing record is
// fine; the first successful Verify adopts the connected instance.
func NewSourceRoster(store SourceStore, logger *slog.Logger) (*SourceRoster, error) {
	if store == nil {
		return nil, errors.New("source store is nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	rec, err := store.Load()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rec != nil && rec.Version > sourceRosterVersion {
		return nil, errors.Errorf("unsupported source roster version %d, supported up to %d", rec.Version, sourceRosterVersion)
	}
	if rec != nil {
		if rec.Current.Flavor == "" && rec.State == SourceStateTrusted {
			rec.Current.Flavor = mysql.MySQLFlavor
		}
		rec.Current = rec.Current.normalized()
		if rec.Pending != nil {
			rec.Pending.Identity = rec.Pending.Identity.normalized()
		}
	}
	return &SourceRoster{
		store:         store,
		logger:        logger,
		rec:           rec,
		flushInterval: DefaultSourceRosterFlushInterval,
	}, nil
}

// SetFlushInterval changes the minimum gap between rewrites of the file caused
// by Checkpoint. State transitions always flush immediately. Zero means every
// checkpoint is flushed.
func (r *SourceRoster) SetFlushInterval(d time.Duration) {
	r.mu.Lock()
	r.flushInterval = d
	r.mu.Unlock()
}

// Verify is called right after every (re)connected dump stream is established,
// before any binlog event from it is accepted.
func (r *SourceRoster) Verify(id SourceIdentity) error {
	id = id.normalized()

	r.mu.Lock()
	defer r.mu.Unlock()

	now := utils.Now()

	if !id.Valid() {
		// Identity cannot be judged. Quarantine what we have, if anything, so a
		// restart cannot silently resume.
		reason := QuarantineReasonUndetermined
		if r.rec != nil && r.rec.State == SourceStateQuarantined && r.rec.Pending != nil &&
			r.rec.Pending.Reason == QuarantineReasonUnknownSource {
			// Preserve the already recorded unknown identity rather than
			// overwriting it with a weaker observation.
			return errors.Trace(ErrUnknownSource)
		}
		if r.rec == nil {
			r.rec = &SourceAttribution{Version: sourceRosterVersion, State: SourceStateQuarantined, BeganAt: now}
		}
		r.rec.State = SourceStateQuarantined
		r.rec.Version = sourceRosterVersion
		r.rec.Pending = &PendingSource{Identity: id, DetectedAt: now, Reason: reason}
		r.rec.UpdatedAt = now
		if err := r.persistLocked(); err != nil {
			return errors.Trace(err)
		}
		r.logger.Error("gtid source identity could not be determined, sync quarantined",
			slog.Any("identity", id))
		return errors.Trace(ErrSourceUndetermined)
	}

	if r.rec == nil {
		// First time we ever see a source: adopt it as trusted.
		r.rec = &SourceAttribution{
			Version:   sourceRosterVersion,
			State:     SourceStateTrusted,
			Current:   id,
			BeganAt:   now,
			LastSeen:  now,
			UpdatedAt: now,
		}
		if err := r.persistLocked(); err != nil {
			return errors.Trace(err)
		}
		r.logger.Info("adopted gtid source as trusted",
			slog.String("server_uuid", id.ServerUUID), slog.Uint64("server_id", uint64(id.ServerID)))
		return nil
	}

	r.rec.Version = sourceRosterVersion

	// A quarantine only clears through an explicit takeover, even if the
	// familiar instance came back.
	if r.rec.State == SourceStateQuarantined {
		if r.rec.Current.Equal(id) {
			return fmt.Errorf("sync still quarantined although source %s is back; explicit takeover required: %w",
				describeIdentity(id), ErrUnknownSource)
		}
		// Refresh the pending observation (address / detection time may change).
		r.rec.Pending = &PendingSource{Identity: id, DetectedAt: now, Reason: QuarantineReasonUnknownSource}
		r.rec.UpdatedAt = now
		if err := r.persistLocked(); err != nil {
			return errors.Trace(err)
		}
		return fmt.Errorf("unknown gtid source %s, sync quarantined pending explicit takeover: %w",
			describeIdentity(id), ErrUnknownSource)
	}

	if r.rec.Current.Equal(id) {
		// Same instance: ordinary reconnect or continued streaming. Addr is not
		// part of identity, but keep the last observed address for forensics.
		r.rec.LastSeen = now
		r.rec.Current.Addr = id.Addr
		return nil
	}

	// Different identity behind the same channel: this is the switchover the
	// roster exists to catch. Do not merge its GTIDs; quarantine.
	r.rec.State = SourceStateQuarantined
	r.rec.Pending = &PendingSource{Identity: id, DetectedAt: now, Reason: QuarantineReasonUnknownSource}
	r.rec.UpdatedAt = now
	if err := r.persistLocked(); err != nil {
		return errors.Trace(err)
	}
	r.logger.Error("gtid source identity changed, sync quarantined",
		slog.String("trusted", describeIdentity(r.rec.Current)),
		slog.String("connected", describeIdentity(id)))
	return fmt.Errorf("connected gtid source %s differs from trusted %s: %w",
		describeIdentity(id), describeIdentity(r.rec.Current), ErrUnknownSource)
}

// Checkpoint records the latest GTID set attributed to the trusted source. It
// is meant to be called at transaction-commit cadence; file rewrites are
// rate-limited by FlushInterval. A checkpoint never leaves quarantine.
func (r *SourceRoster) Checkpoint(gset string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil || r.rec.State != SourceStateTrusted {
		return
	}
	now := utils.Now()
	r.rec.GTIDSet = gset
	r.rec.LastSeen = now
	if r.flushInterval <= 0 || r.lastFlush.IsZero() || now.Sub(r.lastFlush) >= r.flushInterval {
		r.rec.UpdatedAt = now
		if err := r.persistLocked(); err != nil {
			r.logger.Error("failed to persist source roster checkpoint", slog.Any("error", err))
		}
	}
}

// Flush forces the in-memory record onto the store.
func (r *SourceRoster) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil {
		return nil
	}
	r.rec.UpdatedAt = utils.Now()
	return r.persistLocked()
}

// Takeover explicitly trusts a (typically new) instance, closing the current
// tenure into History and clearing quarantine. The high-water GTID set can be
// reset (for example to the new source's gtid_executed) by passing gset.
func (r *SourceRoster) Takeover(id SourceIdentity, gset string) error {
	id = id.normalized()
	if !id.Valid() {
		return errors.Trace(ErrInvalidSourceIdentity)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := utils.Now()
	if r.rec == nil {
		r.rec = &SourceAttribution{Version: sourceRosterVersion, BeganAt: now}
	}
	r.rec.Version = sourceRosterVersion

	// Archive the previous trusted instance unless this is the same identity
	// (e.g. clearing an undetermined quarantine after the familiar box returns).
	// Takeover normally happens while quarantined, so the current state must
	// not gate archiving.
	endReason := QuarantineReasonManualTakeover
	if r.rec.Pending != nil {
		endReason = r.rec.Pending.Reason
	}
	if r.rec.Current.Valid() && !r.rec.Current.Equal(id) {
		r.rec.History = appendTenure(r.rec.History, SourceTenure{
			Identity:  r.rec.Current,
			GTIDSet:   r.rec.GTIDSet,
			BeganAt:   r.rec.BeganAt,
			EndedAt:   now,
			EndReason: endReason,
		})
	}
	// A new identity starts a fresh tenure; taking over for the familiar
	// instance (e.g. after an undetermined scare) keeps the original start.
	if !r.rec.Current.Equal(id) {
		r.rec.BeganAt = now
	}

	r.rec.Current = id
	r.rec.State = SourceStateTrusted
	r.rec.Pending = nil
	r.rec.GTIDSet = gset
	r.rec.LastSeen = now
	r.rec.UpdatedAt = now
	if err := r.persistLocked(); err != nil {
		return errors.Trace(err)
	}
	r.logger.Info("gtid source explicitly taken over",
		slog.String("source", describeIdentity(id)), slog.String("gtid_set", gset))
	return nil
}

// TakeoverPending trusts the identity currently held in quarantine. Use
// Takeover with an explicit identity when the pending observation was an
// undetermined (empty identity) one.
func (r *SourceRoster) TakeoverPending(gset string) error {
	r.mu.Lock()
	pending := r.rec != nil && r.rec.Pending != nil
	var id SourceIdentity
	if pending {
		id = r.rec.Pending.Identity
	}
	r.mu.Unlock()
	if !pending {
		return errors.Trace(ErrNoQuarantinedSource)
	}
	return r.Takeover(id, gset)
}

// State returns the current gating state.
func (r *SourceRoster) State() SourceState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil {
		return ""
	}
	return r.rec.State
}

// Pending returns the suspect identity while quarantined.
func (r *SourceRoster) Pending() (PendingSource, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil || r.rec.Pending == nil {
		return PendingSource{}, false
	}
	return *r.rec.Pending, true
}

// Attribution returns a detached copy of the provenance record.
func (r *SourceRoster) Attribution() SourceAttribution {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rec == nil {
		return SourceAttribution{Version: sourceRosterVersion}
	}
	cp := *r.rec
	if r.rec.History != nil {
		cp.History = append([]SourceTenure(nil), r.rec.History...)
	}
	if r.rec.Pending != nil {
		p := *r.rec.Pending
		cp.Pending = &p
	}
	return cp
}

// persistLocked writes the record and updates lastFlush. Caller holds mu.
func (r *SourceRoster) persistLocked() error {
	if err := r.store.Save(r.rec); err != nil {
		return errors.Trace(err)
	}
	r.lastFlush = utils.Now()
	return nil
}

func appendTenure(history []SourceTenure, t SourceTenure) []SourceTenure {
	// Drop an accidental duplicate of the same tenure; normal append otherwise.
	if n := len(history); n > 0 && history[n-1].Identity.Equal(t.Identity) {
		history[n-1] = t
		return history
	}
	return append(history, t)
}

func describeIdentity(id SourceIdentity) string {
	flavor := id.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	if flavor == mysql.MariaDBFlavor {
		return fmt.Sprintf("mariadb(domain=%d,server_id=%d,uuid=%s)", id.DomainID, id.ServerID, orDash(id.ServerUUID))
	}
	return fmt.Sprintf("mysql(uuid=%s)", orDash(id.ServerUUID))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// FileSourceStore is an atomic JSON-file implementation of SourceStore.
type FileSourceStore struct {
	path string
	mu   sync.Mutex
}

// NewFileSourceStore stores the roster at path (the parent directory is
// created on first save).
func NewFileSourceStore(path string) *FileSourceStore {
	return &FileSourceStore{path: path}
}

// Path returns the configured file location.
func (s *FileSourceStore) Path() string {
	return s.path
}

// Load reads the record. A missing file yields (nil, nil).
func (s *FileSourceStore) Load() (*SourceAttribution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	var rec SourceAttribution
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, errors.Errorf("parse source roster %s: %v", s.path, err)
	}
	return &rec, nil
}

// Save replaces the file atomically via temp file + rename, so a crash never
// leaves a half-written roster.
func (s *FileSourceStore) Save(rec *SourceAttribution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}
	dir := filepath.Dir(s.path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return errors.Trace(err)
		}
	}
	tmp, err := os.CreateTemp(dir, ".source-roster-*")
	if err != nil {
		return errors.Trace(err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return errors.Trace(err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return errors.Trace(err)
	}
	if err := tmp.Close(); err != nil {
		return errors.Trace(err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return errors.Trace(err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// OpenSourceRoster is shorthand for NewSourceRoster(NewFileSourceStore(path)).
func OpenSourceRoster(path string, logger *slog.Logger) (*SourceRoster, error) {
	return NewSourceRoster(NewFileSourceStore(path), logger)
}
