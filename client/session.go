package client

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// maxRestoreAttempts bounds how many physical connections are tried while the
// failure looks like a dead connection. A restore error reported by the server
// (unknown database, privilege error, ...) is not retried.
const maxRestoreAttempts = 3

var (
	// ErrSessionStateUnrecoverable is returned when a Session cannot hand out a
	// physical connection whose session state matches its ledger: the ledger was
	// tainted by a statement that cannot be replayed deterministically, an open
	// transaction would be lost by a connection replacement, or a restore
	// statement (USE / SET NAMES / SET ...) was rejected by the server.
	ErrSessionStateUnrecoverable = stderrors.New("session state cannot be restored safely")

	// ErrSessionClosed is returned when a closed Session is used.
	ErrSessionClosed = stderrors.New("session is closed")
)

type trackedVar struct {
	// value is either a raw SQL expression (replayed verbatim) or a literal
	// string value (quoted when replayed).
	value string
	// literal marks server-reported values that must be quoted on replay.
	literal bool
}

// sessionLedger is the persistent state bookkeeping of one logical session.
// It survives physical connection replacements: every new connection is
// brought up to this state before being delivered.
type sessionLedger struct {
	// hasSchema reports whether schema holds an explicit value; the empty
	// schema ("no default database") is valid.
	hasSchema bool
	schema    string

	charsetMode    charsetMode
	charset        string
	collation      string
	sysVars        map[string]trackedVar
	userVars       map[string]trackedVar
	roleStatements []string

	inTx        bool
	tainted     bool
	taintReason string
}

func newSessionLedger() sessionLedger {
	return sessionLedger{
		sysVars:  make(map[string]trackedVar),
		userVars: make(map[string]trackedVar),
	}
}

func (l *sessionLedger) taint(reason string) {
	if !l.tainted {
		l.tainted = true
		l.taintReason = reason
	}
}

func (l *sessionLedger) deliveryError() error {
	if l.tainted {
		return fmt.Errorf("%w: %s", ErrSessionStateUnrecoverable, l.taintReason)
	}
	return nil
}

// apply folds the classified effects of a successfully executed statement
// into the ledger.
func (l *sessionLedger) apply(effects *parsedEffects) {
	if effects.taint {
		l.taint(effects.taintReason)
		return
	}

	if effects.hasSchema {
		l.hasSchema = true
		l.schema = effects.schema
	}
	if effects.charset.present {
		l.charsetMode = effects.charset.mode
		l.charset = effects.charset.charset
		l.collation = effects.charset.collation
	}
	for _, assignment := range effects.assignments {
		switch assignment.scope {
		case scopeGlobal:
			continue
		case scopeUser:
			l.userVars[assignment.name] = trackedVar{value: assignment.expr}
		default:
			l.sysVars[assignment.name] = trackedVar{value: assignment.expr}
		}
	}
	for _, stmt := range effects.roleStatements {
		if !slices.Contains(l.roleStatements, stmt) {
			l.roleStatements = append(l.roleStatements, stmt)
		}
	}
	switch effects.txResult {
	case 1:
		l.inTx = true
	case 0:
		l.inTx = false
	}
}

// charsetTrackingVars are reported by the server's session tracking for SET
// NAMES / SET CHARACTER SET; they are governed by the ledger's charset state
// and must not be replayed independently.
var charsetTrackingVars = map[string]struct{}{
	"character_set_client":     {},
	"character_set_results":    {},
	"character_set_connection": {},
	"collation_connection":     {},
}

// foldTracking incorporates server-reported session-state changes (available
// when CLIENT_SESSION_TRACK was negotiated). It is authoritative for changes
// made without a recognizable SET statement (stored procedures, triggers, ...).
func (l *sessionLedger) foldTracking(info *mysql.SessionTrackingInfo) {
	if info == nil {
		return
	}
	if info.Schema != "" {
		l.hasSchema = true
		l.schema = info.Schema
	}
	for name, value := range info.Variables {
		name = strings.ToLower(name)
		if _, skip := charsetTrackingVars[name]; skip {
			continue
		}
		l.sysVars[name] = trackedVar{value: value, literal: true}
	}
}

// Session is a logical database connection over a Pool. It keeps a persistent
// ledger of the session-scoped state that matters across physical connection
// replacements (default database, connection character set and tracked
// session variables). Whenever a physical connection is checked out, the
// ledger state is restored on it before the connection is delivered.
//
// State changes must go through the Session (UseDB, SetNames, SetSessionVar,
// Execute with USE/SET, ...). When state cannot be proven replayable, the
// ledger is tainted and further GetConn calls fail with
// ErrSessionStateUnrecoverable instead of silently delivering a connection
// with default state.
//
// A Session serializes its own use (like a single logical connection): at most
// one connection may be checked out at a time. Different Session instances
// are fully isolated; each restores its own state. Connections returned with
// PutConn are reset to pool defaults so they can be safely reused by other
// sessions.
type Session struct {
	pool *Pool

	mu     sync.Mutex
	ledger sessionLedger
	conn   *Conn
	closed bool

	// Baseline state of a freshly created pool connection, captured on first
	// checkout and used to reset connections on return.
	baselineSet       bool
	baselineSchema    string
	baselineCharset   string
	baselineCollation string
}

// SessionStateView is a point-in-time, detached copy of the session ledger.
type SessionStateView struct {
	// Schema is the tracked default database.
	Schema string
	// Charset is the tracked connection character set.
	Charset string
	// Collation is the tracked connection collation, if set with SET NAMES.
	Collation string
	// SessionVars maps tracked SESSION system variable names to their stored
	// SQL value (either a raw expression or a server-reported literal).
	SessionVars map[string]string
	// UserVars maps tracked user variable names (without @) to stored values.
	UserVars map[string]string
	// InTransaction reports whether the logical session is in a transaction.
	InTransaction bool
	// Tainted reports whether the ledger contains state it cannot replay.
	Tainted bool
	// TaintReason explains why the ledger was tainted.
	TaintReason string
}

// NewSession creates a logical session bound to the pool.
func (pool *Pool) NewSession() *Session {
	return &Session{
		pool:   pool,
		ledger: newSessionLedger(),
	}
}

// GetConn checks out a physical connection and restores the ledger state on it
// (default database, character set, session variables) before returning.
// If state cannot be restored safely the connection is not delivered and
// ErrSessionStateUnrecoverable is returned.
func (s *Session) GetConn(ctx context.Context) (*Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSessionClosed
	}
	if s.conn != nil {
		return nil, fmt.Errorf("session connection is already checked out")
	}
	if err := s.ledger.deliveryError(); err != nil {
		return nil, err
	}

	var lastErr error
	for range maxRestoreAttempts {
		conn, err := s.pool.GetConn(ctx)
		if err != nil {
			return nil, err
		}

		if !s.baselineSet {
			s.captureBaseline(conn)
		}

		if s.ledger.inTx {
			// A transaction is bound to a specific physical connection and can
			// neither be replayed nor safely moved: refuse delivery.
			s.pool.PutConn(conn)
			return nil, fmt.Errorf("%w: open transaction cannot be moved to a replacement connection", ErrSessionStateUnrecoverable)
		}

		if err := s.restoreLocked(conn); err != nil {
			s.pool.DropConn(conn)

			var myErr *mysql.MyError
			if stderrors.As(err, &myErr) {
				// A server-side rejection (unknown database, missing privilege,
				// invalid value, ...) would fail the same way on every attempt.
				return nil, fmt.Errorf("%w: %v", ErrSessionStateUnrecoverable, err)
			}
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// The connection was dead: try another one.
			continue
		}

		s.conn = conn
		return conn, nil
	}

	return nil, fmt.Errorf("%w: state restore failed after %d attempts: %v", ErrSessionStateUnrecoverable, maxRestoreAttempts, lastErr)
}

// PutConn returns the checked-out connection to the pool. Before that, all
// tracked state is reset to pool defaults so another session cannot observe
// it. If the connection is in a transaction or cannot be reset, it is closed
// instead of being reused.
func (s *Session) PutConn(conn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || conn != s.conn {
		// Foreign/unknown connection: never make it available to others.
		s.pool.DropConn(conn)
		return
	}
	s.conn = nil

	if s.ledger.inTx || conn.IsInTransaction() {
		// The logical session still owns a transaction; returning a connection
		// with an open transaction (or resetting it) would silently change
		// semantics, so the physical connection is discarded. The ledger keeps
		// inTx set and the next checkout is refused.
		s.ledger.inTx = true
		s.pool.DropConn(conn)
		return
	}

	if err := s.resetLocked(conn); err != nil {
		s.pool.DropConn(conn)
		return
	}
	s.pool.PutConn(conn)
}

// DropConn closes the physical connection without returning it to the pool.
// The ledger is preserved: the next checkout gets a fresh connection with the
// state replayed, except when a transaction was open, in which case checkout
// is refused.
func (s *Session) DropConn(conn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if conn == s.conn {
		s.conn = nil
	}
	s.pool.DropConn(conn)
}

// Close releases the session. A checked-out connection is closed.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	if s.conn != nil {
		s.pool.DropConn(s.conn)
		s.conn = nil
	}
}

// Reset clears the ledger back to pool defaults and clears a tainted or
// transaction state. No connection may be checked out. Use it only when the
// caller deliberately starts a fresh logical state.
func (s *Session) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil {
		return fmt.Errorf("cannot reset session while a connection is checked out")
	}
	s.ledger = newSessionLedger()
	if s.baselineSet {
		s.seedBaselineLedger()
	}
	return nil
}

// State returns a detached snapshot of the current ledger state.
func (s *Session) State() SessionStateView {
	s.mu.Lock()
	defer s.mu.Unlock()

	view := SessionStateView{
		Schema:        s.ledger.schema,
		Charset:       s.ledger.charset,
		Collation:     s.ledger.collation,
		SessionVars:   make(map[string]string, len(s.ledger.sysVars)),
		UserVars:      make(map[string]string, len(s.ledger.userVars)),
		InTransaction: s.ledger.inTx,
		Tainted:       s.ledger.tainted,
		TaintReason:   s.ledger.taintReason,
	}
	for name, v := range s.ledger.sysVars {
		view.SessionVars[name] = v.value
	}
	for name, v := range s.ledger.userVars {
		view.UserVars[name] = v.value
	}
	return view
}

func (s *Session) captureBaseline(conn *Conn) {
	s.baselineSet = true
	s.baselineSchema = conn.GetDB()
	s.baselineCharset = conn.GetCharset()
	s.baselineCollation = conn.GetCollation()
	s.seedBaselineLedger()
}

// seedBaselineLedger initializes ledger entries that mirror a fresh pool
// connection so restore can skip unchanged baseline state.
func (s *Session) seedBaselineLedger() {
	s.ledger.schema = s.baselineSchema
	s.ledger.hasSchema = s.baselineSchema != ""
	s.ledger.charset = s.baselineCharset
	s.ledger.collation = s.baselineCollation
	s.ledger.charsetMode = charsetModeUnspecified
}

// UseDB changes the default database, both on the physical connection and in
// the ledger.
func (s *Session) UseDB(schema string) error {
	effects := classifyQuery("USE " + quoteIdent(schema))
	if effects.taint {
		return fmt.Errorf("%w: invalid database name %q", ErrSessionStateUnrecoverable, schema)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.UseDBWithResult(schema); err != nil {
		return err
	}
	s.ledger.apply(&effects)
	return nil
}

// SetCharset issues "SET NAMES <charset>" and records it in the ledger.
func (s *Session) SetCharset(charsetName string) error {
	return s.SetNames(charsetName, "")
}

// SetNames issues "SET NAMES <charset> [COLLATE <collation>]" and records it.
func (s *Session) SetNames(charsetName, collation string) error {
	if !isSimpleSQLName(charsetName) || (collation != "" && !isSimpleSQLName(collation)) {
		return fmt.Errorf("invalid charset or collation name")
	}

	stmt := "SET NAMES " + charsetName
	if collation != "" {
		stmt += " COLLATE " + collation
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	conn.charset = charsetName
	conn.collation = collation
	effects := classifyQuery(stmt)
	s.ledger.apply(&effects)
	return nil
}

// SetCharacterSet issues "SET CHARACTER SET <charset>" and records it.
func (s *Session) SetCharacterSet(charsetName string) error {
	if !isSimpleSQLName(charsetName) {
		return fmt.Errorf("invalid charset name")
	}
	stmt := "SET CHARACTER SET " + charsetName

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	conn.charset = charsetName
	conn.collation = ""
	effects := classifyQuery(stmt)
	s.ledger.apply(&effects)
	return nil
}

// SetSessionVar sets a SESSION system variable. valueExpr is a raw SQL
// expression replayed verbatim on replacement connections; it is rejected up
// front when it cannot be guaranteed to replay identically (e.g. it references
// other variables or non-deterministic functions).
func (s *Session) SetSessionVar(name, valueExpr string) error {
	if !isSimpleSQLName(name) {
		return fmt.Errorf("invalid session variable name %q", name)
	}
	if unsafe, reason := exprReplayUnsafe(valueExpr); unsafe {
		return fmt.Errorf("%w: %s", ErrSessionStateUnrecoverable, reason)
	}
	stmt := "SET SESSION " + quoteIdent(name) + " = " + valueExpr

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	effects := classifyQuery(stmt)
	s.ledger.apply(&effects)
	return nil
}

// SetSessionVarString sets a SESSION system variable to a string literal. The
// value is quoted and escaped, so arbitrary text is safe.
func (s *Session) SetSessionVarString(name, value string) error {
	if !isSimpleSQLName(name) {
		return fmt.Errorf("invalid session variable name %q", name)
	}
	stmt := "SET SESSION " + quoteIdent(name) + " = " + quoteSQLString(value)

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	s.ledger.sysVars[strings.ToLower(name)] = trackedVar{value: value, literal: true}
	return nil
}

// SetUserVar sets a user variable (@name). The same replay-safety rules as for
// SetSessionVar apply.
func (s *Session) SetUserVar(name, valueExpr string) error {
	if !isValidUserVarName(name) {
		return fmt.Errorf("invalid user variable name %q", name)
	}
	if unsafe, reason := exprReplayUnsafe(valueExpr); unsafe {
		return fmt.Errorf("%w: %s", ErrSessionStateUnrecoverable, reason)
	}
	stmt := "SET @" + name + " = " + valueExpr

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	s.ledger.userVars[name] = trackedVar{value: valueExpr}
	return nil
}

// SetUserVarString sets a user variable to a quoted string literal.
func (s *Session) SetUserVarString(name, value string) error {
	if !isValidUserVarName(name) {
		return fmt.Errorf("invalid user variable name %q", name)
	}
	stmt := "SET @" + name + " = " + quoteSQLString(value)

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if _, err := conn.exec(stmt); err != nil {
		return err
	}
	s.ledger.userVars[name] = trackedVar{value: value, literal: true}
	return nil
}

// Begin starts a transaction on the logical session.
func (s *Session) Begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if err := conn.Begin(); err != nil {
		return err
	}
	s.ledger.inTx = conn.IsInTransaction()
	return nil
}

// Commit commits the current transaction.
func (s *Session) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if err := conn.Commit(); err != nil {
		return err
	}
	s.ledger.inTx = conn.IsInTransaction()
	return nil
}

// Rollback rolls back the current transaction.
func (s *Session) Rollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return err
	}
	if err := conn.Rollback(); err != nil {
		return err
	}
	s.ledger.inTx = conn.IsInTransaction()
	return nil
}

// Execute runs a query on the logical session. USE, SET NAMES, SET
// SESSION/USER variable assignments and transaction control statements are
// recognized and mirrored into the ledger; unrecognized state-changing SET
// forms taint the ledger so a later checkout fails safely.
func (s *Session) Execute(query string, args ...any) (*mysql.Result, error) {
	effects := classifyQuery(query)

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.activeConnLocked()
	if err != nil {
		return nil, err
	}

	result, err := conn.Execute(query, args...)
	if err != nil {
		return result, err
	}

	s.ledger.apply(&effects)
	s.ledger.inTx = conn.IsInTransaction()
	s.ledger.foldTracking(result.SessionTracking)

	// Keep the connection's client-side state cache consistent with the raw
	// USE / SET NAMES statements that just executed.
	if effects.hasSchema {
		conn.db = effects.schema
	}
	if effects.charset.present {
		conn.charset = effects.charset.charset
		conn.collation = effects.charset.collation
	}
	return result, nil
}

func (s *Session) activeConnLocked() (*Conn, error) {
	if s.closed {
		return nil, ErrSessionClosed
	}
	if s.conn == nil {
		return nil, fmt.Errorf("no connection checked out for the session; call GetConn first")
	}
	return s.conn, nil
}

// restoreLocked brings a freshly checked-out connection up to ledger state.
// Caller must not hold a connection that is in a transaction (checked by
// GetConn before calling).
func (s *Session) restoreLocked(conn *Conn) error {
	ledger := &s.ledger

	if ledger.hasSchema && conn.GetDB() != ledger.schema {
		if _, err := conn.UseDBWithResult(ledger.schema); err != nil {
			return err
		}
	}

	if ledger.charsetMode != charsetModeUnspecified ||
		ledger.charset != s.baselineCharset ||
		ledger.collation != s.baselineCollation {
		switch ledger.charsetMode {
		case charsetModeNames:
			stmt := "SET NAMES " + ledger.charset
			if ledger.collation != "" {
				stmt += " COLLATE " + ledger.collation
			}
			if _, err := conn.exec(stmt); err != nil {
				return err
			}
		case charsetModeCharset:
			if _, err := conn.exec("SET CHARACTER SET " + ledger.charset); err != nil {
				return err
			}
		}
		conn.charset = ledger.charset
		conn.collation = ledger.collation
	}

	for _, stmt := range ledger.roleStatements {
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
	}

	if stmt := buildSysVarStatement(ledger.sysVars, false); stmt != "" {
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
	}
	if stmt := buildUserVarStatement(ledger.userVars, false); stmt != "" {
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// resetLocked returns a physical connection to pool baseline state before it is
// made available to another session. Any failure means the connection must be
// closed rather than reused.
func (s *Session) resetLocked(conn *Conn) error {
	ledger := &s.ledger

	if stmt := buildSysVarStatement(ledger.sysVars, true); stmt != "" {
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
	}
	if stmt := buildUserVarStatement(ledger.userVars, true); stmt != "" {
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
	}
	if len(ledger.roleStatements) > 0 {
		if _, err := conn.exec("SET ROLE NONE"); err != nil {
			return err
		}
	}

	if ledger.charsetMode != charsetModeUnspecified ||
		ledger.charset != s.baselineCharset ||
		ledger.collation != s.baselineCollation {
		stmt := "SET NAMES " + s.baselineCharset
		if s.baselineCollation != "" {
			stmt += " COLLATE " + s.baselineCollation
		}
		if _, err := conn.exec(stmt); err != nil {
			return err
		}
		conn.charset = s.baselineCharset
		conn.collation = s.baselineCollation
	}

	if ledger.hasSchema && ledger.schema != s.baselineSchema {
		if s.baselineSchema == "" {
			// MySQL provides no way to clear the default schema.
			return fmt.Errorf("cannot reset default schema on connection")
		}
		if _, err := conn.UseDBWithResult(s.baselineSchema); err != nil {
			return err
		}
	}
	return nil
}

// buildSysVarStatement renders one SET statement for system variables, either
// replaying ledger values (reset=false) or resetting them to DEFAULT.
func buildSysVarStatement(vars map[string]trackedVar, reset bool) string {
	if len(vars) == 0 {
		return ""
	}
	names := make([]string, 0, len(vars))
	for name := range vars {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		if reset {
			parts = append(parts, quoteIdent(name)+" = DEFAULT")
			continue
		}
		v := vars[name]
		value := v.value
		if v.literal {
			value = quoteSQLString(value)
		}
		parts = append(parts, quoteIdent(name)+" = "+value)
	}
	return "SET SESSION " + strings.Join(parts, ", ")
}

// buildUserVarStatement renders one SET statement for user variables, either
// replaying ledger values or resetting them to NULL (the uninitialized value).
func buildUserVarStatement(vars map[string]trackedVar, reset bool) string {
	if len(vars) == 0 {
		return ""
	}
	names := make([]string, 0, len(vars))
	for name := range vars {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		if reset {
			parts = append(parts, "@"+name+" = NULL")
			continue
		}
		v := vars[name]
		value := v.value
		if v.literal {
			value = quoteSQLString(value)
		}
		parts = append(parts, "@"+name+" = "+value)
	}
	return "SET " + strings.Join(parts, ", ")
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteSQLString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString("''")
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteByte(value[i])
		}
	}
	b.WriteByte('\'')
	return b.String()
}

func isValidUserVarName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isUserVarNameByte(name[i]) {
			return false
		}
	}
	return true
}
