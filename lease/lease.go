// Package lease implements persistent, fail-closed leases for downstream
// binlog replicas connected to a go-mysql based server.
//
// A MySQL replication client is only required to announce a server id, and two
// independently deployed replicas can therefore end up configured with the
// same one. Without additional bookkeeping both can stream binlog events at
// the same time, and after a disconnect it is impossible to tell which
// instance is allowed to resume. This package binds every replicated server id
// to a stable replica instance identity and records the connection generation
// and last requested binlog position.
//
// The admission rules implemented by [Manager.Admit] are deliberately
// conservative:
//
//   - No lease exists for the server id: the claim is unambiguous, a new lease
//     is granted at generation 1.
//   - The claimant has the same instance identity as the lease owner: it may
//     resume the existing lease. The previous live session (if any) is fenced
//     and the generation is advanced.
//   - A different instance claims a server id while another session is live:
//     this is a concurrent duplicate and ownership cannot be decided, so the
//     server id is quarantined, both sessions are fenced and streaming must
//     not start.
//   - A different instance claims a server id whose owner is currently away
//     (disconnected or observed after a process restart): the identity is
//     unknown relative to the persisted binding, so the server id is
//     quarantined instead of silently handing the lease over.
//   - While quarantined, only the previously bound owner is allowed to resume
//     implicitly. Switching the owner to another instance requires an explicit
//     [Manager.Takeover].
//
// Whenever the persisted state is missing, corrupt or cannot be written, the
// manager refuses to admit the connection rather than guessing.
package lease

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Status of a persisted lease.
type Status string

const (
	// StatusActive means the lease is bound to an owner instance. It may or
	// may not have a live network session at any given moment.
	StatusActive Status = "active"

	// StatusQuarantined means ownership of the server id is ambiguous. No new
	// session may stream until the original owner reconnects or an operator
	// performs an explicit takeover.
	StatusQuarantined Status = "quarantined"
)

// Quarantine and revocation reasons.
const (
	// ReasonDuplicateConcurrent is recorded when two different instances claim
	// the same server id while a session is live.
	ReasonDuplicateConcurrent = "duplicate_concurrent"

	// ReasonUnknownInstance is recorded when an instance that is not the bound
	// owner claims a server id without an explicit takeover.
	ReasonUnknownInstance = "unknown_instance"

	// RevocationReasonSuperseded notifies a session that the same instance
	// reconnected and replaced it.
	RevocationReasonSuperseded = "superseded"

	// RevocationReasonTakeover notifies a session that an operator explicitly
	// transferred the lease.
	RevocationReasonTakeover = "explicit_takeover"

	// RevocationReasonExpunged notifies a session that the lease was deleted.
	RevocationReasonExpunged = "expunged"
)

// Sentinel errors. Callers should test with errors.Is.
var (
	// ErrInvalidArgument is returned for malformed requests (zero server id,
	// zero location, ...).
	ErrInvalidArgument = errors.New("lease: invalid argument")

	// ErrNotLoaded is returned when the manager has not successfully loaded the
	// persisted state. Admission is refused instead of guessing.
	ErrNotLoaded = errors.New("lease: manager is not loaded")

	// ErrUnknownIdentity is returned when a claim carries no stable instance
	// identity. Such claims must never be admitted.
	ErrUnknownIdentity = errors.New("lease: replica identity is unknown")

	// ErrQuarantined is returned when admission is denied because the server id
	// is quarantined. An AdmissionError may wrap it with more details.
	ErrQuarantined = errors.New("lease: server id is quarantined")

	// ErrLeaseNotFound is returned when no persisted lease exists for a server
	// id.
	ErrLeaseNotFound = errors.New("lease: lease not found")

	// ErrSessionRevoked is returned by session operations after the session was
	// fenced by a newer generation, an explicit takeover or an expunge.
	ErrSessionRevoked = errors.New("lease: session revoked")
)

// Identity identifies a downstream replica.
//
// ServerID is the MySQL replication server id announced in COM_BINLOG_DUMP.
// InstanceID is the stable identity of the deployed replica process, e.g. a
// deployment UUID or the registered host:port. It cannot be derived from the
// server id because two replicas may share one, and it must stay stable across
// reconnects of the same instance.
type Identity struct {
	ServerID   uint32 `json:"server_id"`
	InstanceID string `json:"instance_id"`
}

func (i Identity) validate() error {
	if i.ServerID == 0 {
		return fmt.Errorf("%w: server id must not be zero", ErrInvalidArgument)
	}
	if strings.TrimSpace(i.InstanceID) == "" {
		return fmt.Errorf("%w: instance id for server id %d is empty", ErrUnknownIdentity, i.ServerID)
	}
	return nil
}

// Location is the persisted replication position of a replica: either a
// binlog file/offset pair, a GTID set, or both.
type Location struct {
	BinlogName string `json:"binlog_name,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`
	GTIDSet    string `json:"gtid_set,omitempty"`
}

// IsZero reports whether the location carries no position information at all.
func (l Location) IsZero() bool {
	return l.BinlogName == "" && l.BinlogPos == 0 && l.GTIDSet == ""
}

// Position returns the file/offset part as a mysql.Position.
func (l Location) Position() mysql.Position {
	return mysql.Position{Name: l.BinlogName, Pos: l.BinlogPos}
}

func (l Location) String() string {
	if l.GTIDSet != "" {
		return fmt.Sprintf("(%s, %d, %s)", l.BinlogName, l.BinlogPos, l.GTIDSet)
	}
	return fmt.Sprintf("(%s, %d)", l.BinlogName, l.BinlogPos)
}

// Candidate records a claim observed while a lease was quarantined, preserving
// the evidence needed to decide who should continue pulling.
type Candidate struct {
	InstanceID string    `json:"instance_id"`
	Location   Location  `json:"location,omitzero"`
	ObservedAt time.Time `json:"observed_at"`
}

// Quarantine describes why a server id is isolated and which instances were
// observed contending for it.
type Quarantine struct {
	Reason     string      `json:"reason"`
	Since      time.Time   `json:"since"`
	Detail     string      `json:"detail,omitempty"`
	Candidates []Candidate `json:"candidates,omitempty"`
}

func (q *Quarantine) addCandidate(instanceID string, loc Location, at time.Time) {
	for i := range q.Candidates {
		if q.Candidates[i].InstanceID == instanceID {
			q.Candidates[i].Location = loc
			q.Candidates[i].ObservedAt = at
			return
		}
	}
	q.Candidates = append(q.Candidates, Candidate{
		InstanceID: instanceID,
		Location:   loc,
		ObservedAt: at,
	})
}

// Takeover records the last explicit ownership transfer of a lease.
type Takeover struct {
	By             string    `json:"by"`
	Reason         string    `json:"reason,omitempty"`
	At             time.Time `json:"at"`
	PreviousOwner  string    `json:"previous_owner,omitempty"`
	FromGeneration uint64    `json:"from_generation"`
}

// Record is the persistent state of one replicated server id.
type Record struct {
	// ServerID is the replication server id the lease is keyed by.
	ServerID uint32 `json:"server_id"`

	// OwnerInstanceID is the stable identity of the instance currently bound
	// to the server id.
	OwnerInstanceID string `json:"owner_instance_id"`

	// Generation is a monotonically increasing epoch. It is bumped every time
	// a session is granted or ownership is transferred and acts as a fencing
	// token for sessions created before the bump.
	Generation uint64 `json:"generation"`

	// Status is active or quarantined.
	Status Status `json:"status"`

	// LastLocation is the newest requested/acknowledged replication position.
	LastLocation Location `json:"last_location"`

	// CreatedAt is when the lease was first granted.
	CreatedAt time.Time `json:"created_at"`

	// LastSeenAt is the last admit, heartbeat, advance or release time.
	LastSeenAt time.Time `json:"last_seen_at"`

	// Quarantine is set while the lease is isolated.
	Quarantine *Quarantine `json:"quarantine,omitempty"`

	// LastTakeover records the most recent explicit ownership transfer.
	LastTakeover *Takeover `json:"last_takeover,omitempty"`
}

// AdmissionError is returned when a claim is denied. It wraps one of the
// sentinel errors and carries the evidence behind the decision.
type AdmissionError struct {
	// Reason is one of the Reason* constants when Err is ErrQuarantined.
	Reason string
	// ServerID is the contended server id.
	ServerID uint32
	// InstanceID is the claimant that was rejected.
	InstanceID string
	// Err is the underlying sentinel error.
	Err error
}

func (e *AdmissionError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("lease: server id %d claim by %q denied (%s): %v",
			e.ServerID, e.InstanceID, e.Reason, e.Err)
	}
	return fmt.Sprintf("lease: server id %d claim by %q denied: %v",
		e.ServerID, e.InstanceID, e.Err)
}

// Unwrap enables errors.Is against the wrapped sentinel error.
func (e *AdmissionError) Unwrap() error { return e.Err }

func cloneRecord(r *Record) *Record {
	cp := *r
	if r.Quarantine != nil {
		q := *r.Quarantine
		q.Candidates = append([]Candidate(nil), r.Quarantine.Candidates...)
		cp.Quarantine = &q
	}
	if r.LastTakeover != nil {
		t := *r.LastTakeover
		cp.LastTakeover = &t
	}
	return &cp
}

// mergeLocation returns the further ahead of cur and next. The binlog
// file/offset is authoritative; when they are equal, a GTID set on next is
// preserved.
func mergeLocation(cur, next Location) Location {
	if next.IsZero() {
		return cur
	}
	if cur.IsZero() {
		return next
	}
	switch next.Position().Compare(cur.Position()) {
	case 1:
		return next
	case -1:
		return cur
	default:
		out := cur
		if next.GTIDSet != "" {
			out.GTIDSet = next.GTIDSet
		}
		return out
	}
}
