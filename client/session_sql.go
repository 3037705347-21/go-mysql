package client

import (
	"strings"
)

// This file contains a deliberately small, conservative classifier for the
// statements that can change session-scoped connection state:
//
//   - USE <db>
//   - SET NAMES ... / SET CHARACTER SET ...
//   - SET [SESSION|LOCAL|GLOBAL|@@|@] <var> = <expr>
//   - BEGIN / START TRANSACTION / COMMIT / ROLLBACK
//
// It never tries to fully parse SQL. Its only job is deciding whether a query
// that succeeded on the current physical connection can be replayed on a
// replacement connection. Anything the classifier cannot prove safe marks the
// session ledger as tainted, so a later checkout fails loudly instead of
// silently delivering a connection with default state.

type charsetMode uint8

const (
	charsetModeUnspecified charsetMode = iota
	// charsetModeNames replays "SET NAMES <charset> [COLLATE <collation>]".
	charsetModeNames
	// charsetModeCharset replays "SET CHARACTER SET <charset>".
	charsetModeCharset
)

type varScope uint8

const (
	scopeSession varScope = iota
	scopeGlobal
	scopeUser
)

type charsetChange struct {
	mode      charsetMode
	charset   string
	collation string
	present   bool
}

type varAssignment struct {
	scope varScope
	// name is the lower-cased system variable name for scopeSession/scopeGlobal,
	// or the user variable name as written (without leading @) for scopeUser.
	name string
	// expr is the raw, already-server-accepted value expression, replayed
	// verbatim on a replacement connection.
	expr string
	// unsafe means the expression cannot be guaranteed to evaluate to the same
	// value on a fresh connection (it references other variables, uses a
	// non-deterministic function, ...), so the ledger must be tainted once the
	// statement has succeeded.
	unsafe bool
}

// parsedEffects is the state delta a query may produce. It is applied to the
// session ledger only after the query executed successfully.
type parsedEffects struct {
	hasSchema bool
	schema    string

	charset charsetChange

	// roleStatements are exact "SET ROLE ..." statements to replay verbatim.
	roleStatements []string

	assignments []varAssignment

	// txResult is -1: unchanged, 0: transaction ended, 1: transaction started.
	txResult int8

	taint       bool
	taintReason string
}

func (e *parsedEffects) taintf(reason string) {
	if !e.taint {
		e.taint = true
		e.taintReason = reason
	}
}

// classifyQuery inspects a single query text. Multi-statement text
// ("USE a; SET x = 1") is supported: every statement is classified in order.
func classifyQuery(query string) parsedEffects {
	effects := parsedEffects{txResult: -1}

	stmts := splitStatements(query)
	for _, stmt := range stmts {
		if isBlankStatement(stmt) {
			continue
		}
		classifyStatement(stmt, &effects)
		if effects.taint {
			return effects
		}
	}
	return effects
}

func classifyStatement(stmt string, effects *parsedEffects) {
	s := newWordScanner(stmt)
	keyword := s.readWord()
	if keyword == "" {
		return
	}

	switch strings.ToLower(keyword) {
	case "use":
		name, ok := s.readSimpleIdentifierTail()
		if !ok || s.hasMore() {
			effects.taintf("unparseable USE statement: " + strings.TrimSpace(stmt))
			return
		}
		effects.hasSchema = true
		effects.schema = name
	case "begin":
		// "BEGIN [WORK]" starts a transaction, there is no other valid form.
		if s.hasMore() {
			if !strings.EqualFold(s.readWord(), "work") || s.hasMore() {
				effects.taintf("unparseable BEGIN statement: " + strings.TrimSpace(stmt))
				return
			}
		}
		effects.txResult = 1
	case "start":
		if strings.ToLower(s.readWord()) != "transaction" {
			effects.taintf("unparseable START statement: " + strings.TrimSpace(stmt))
			return
		}
		effects.txResult = 1
	case "commit":
		effects.txResult = 0
	case "rollback":
		// "ROLLBACK TO SAVEPOINT x" does not end the transaction.
		next := strings.ToLower(s.peekWord())
		if next == "to" {
			return
		}
		effects.txResult = 0
	case "set":
		classifySet(&s, stmt, effects)
	case "xa":
		effects.taintf("XA transactions cannot be replayed on a replacement connection")
	case "lock":
		if strings.EqualFold(s.peekWord(), "tables") {
			effects.taintf("LOCK TABLES state cannot be replayed on a replacement connection")
		}
	}
}

func classifySet(s *wordScanner, original string, effects *parsedEffects) {
	first := strings.ToLower(s.peekWord())

	switch first {
	case "names":
		s.readWord()
		cs := s.readWord()
		if !isSimpleSQLName(cs) {
			effects.taintf("unparseable SET NAMES charset: " + strings.TrimSpace(original))
			return
		}
		change := charsetChange{mode: charsetModeNames, charset: cs, present: true}
		if s.hasMore() {
			if !strings.EqualFold(s.readWord(), "collate") {
				effects.taintf("unparseable SET NAMES statement: " + strings.TrimSpace(original))
				return
			}
			col := s.readWord()
			if !isSimpleSQLName(col) || s.hasMore() {
				effects.taintf("unparseable SET NAMES collation: " + strings.TrimSpace(original))
				return
			}
			change.collation = col
		}
		effects.charset = change
		return
	case "character":
		s.readWord()
		if !strings.EqualFold(s.readWord(), "set") {
			effects.taintf("unparseable SET statement: " + strings.TrimSpace(original))
			return
		}
		cs := s.readWord()
		if !isSimpleSQLName(cs) || s.hasMore() {
			effects.taintf("unparseable SET CHARACTER SET statement: " + strings.TrimSpace(original))
			return
		}
		effects.charset = charsetChange{mode: charsetModeCharset, charset: cs, present: true}
		return
	case "charset":
		s.readWord()
		cs := s.readWord()
		if !isSimpleSQLName(cs) || s.hasMore() {
			effects.taintf("unparseable SET CHARSET statement: " + strings.TrimSpace(original))
			return
		}
		effects.charset = charsetChange{mode: charsetModeCharset, charset: cs, present: true}
		return
	case "role":
		// "SET ROLE ..." is fully session scoped and replayable verbatim.
		s.readWord()
		rest := strings.TrimSpace(s.rest())
		if rest == "" {
			effects.taintf("unparseable SET ROLE statement: " + strings.TrimSpace(original))
			return
		}
		effects.roleStatements = append(effects.roleStatements, "SET ROLE "+rest)
		return
	case "transaction":
		// "SET TRANSACTION ..." only applies to the next transaction and does
		// not survive a connection replacement, so there is nothing to replay.
		return
	case "session":
		s.readWord()
		if strings.EqualFold(s.peekWord(), "characteristics") {
			// Could be replayed verbatim, but its reset semantics on a returned
			// connection are ambiguous; refuse instead of guessing.
			effects.taintf("SET SESSION CHARACTERISTICS cannot be safely replayed: " + strings.TrimSpace(original))
			return
		}
	case "global", "persist", "persist_only":
		// Server-global state is not tied to the connection.
	case "password", "default":
		// Account management (SET PASSWORD ..., SET DEFAULT ROLE ...), no
		// session state.
		return
	}

	for _, segment := range splitTopLevel(s.rest(), ',') {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			effects.taintf("empty SET assignment")
			return
		}
		assignment, ok := parseSetAssignment(segment)
		if !ok {
			effects.taintf("unparseable SET assignment: " + segment)
			return
		}
		if assignment.scope == scopeGlobal {
			// Global state is not tied to the physical connection.
			continue
		}
		if assignment.unsafe {
			effects.taintf("SET value cannot be guaranteed to replay identically: " + segment)
			return
		}
		effects.assignments = append(effects.assignments, assignment)
	}
}

func parseSetAssignment(segment string) (varAssignment, bool) {
	s := newWordScanner(segment)

	if s.peekByte() == '@' {
		s.readByte()
		if s.peekByte() == '@' {
			// "@@...": system variable with an optional scope prefix.
			s.readByte()
			return parseAtAtSystemVar(&s)
		}
		// "@name": user variable.
		name, ok := readUserVarName(&s)
		if !ok {
			return varAssignment{}, false
		}
		expr, unsafe, ok := readAssignmentExpr(&s)
		if !ok {
			return varAssignment{}, false
		}
		return varAssignment{scope: scopeUser, name: name, expr: expr, unsafe: unsafe}, true
	}

	scope := scopeSession
	switch strings.ToLower(s.peekWord()) {
	case "global":
		scope = scopeGlobal
		s.readWord()
	case "session", "local":
		s.readWord()
	case "persist", "persist_only":
		scope = scopeGlobal
		s.readWord()
	}

	name, ok := s.readBareIdentifier()
	if !ok {
		return varAssignment{}, false
	}
	expr, unsafe, ok := readAssignmentExpr(&s)
	if !ok {
		return varAssignment{}, false
	}
	return varAssignment{scope: scope, name: strings.ToLower(name), expr: expr, unsafe: unsafe}, true
}

func parseAtAtSystemVar(s *wordScanner) (varAssignment, bool) {
	scope := scopeSession
	switch strings.ToLower(s.peekWord()) {
	case "global":
		scope = scopeGlobal
		s.readWord()
		if s.peekByte() != '.' {
			return varAssignment{}, false
		}
		s.readByte()
	case "session", "local":
		s.readWord()
		if s.peekByte() != '.' {
			return varAssignment{}, false
		}
		s.readByte()
	}

	name, ok := s.readBareIdentifier()
	if !ok {
		return varAssignment{}, false
	}
	expr, unsafe, ok := readAssignmentExpr(s)
	if !ok {
		return varAssignment{}, false
	}
	return varAssignment{scope: scope, name: strings.ToLower(name), expr: expr, unsafe: unsafe}, true
}

func readAssignmentExpr(s *wordScanner) (expr string, unsafe bool, ok bool) {
	op1 := s.peekByte()
	switch {
	case op1 == '=':
		s.readByte()
	case op1 == ':' && s.peekByteAt(1) == '=':
		s.readByte()
		s.readByte()
	default:
		return "", false, false
	}
	expr = strings.TrimSpace(s.rest())
	if expr == "" {
		return "", false, false
	}
	unsafe, _ = exprReplayUnsafe(expr)
	return expr, unsafe, true
}

func readUserVarName(s *wordScanner) (string, bool) {
	start := s.p
	for s.p < len(s.b) {
		c := s.b[s.p]
		if isUserVarNameByte(c) {
			s.p++
			continue
		}
		break
	}
	name := string(s.b[start:s.p])
	if name == "" {
		return "", false
	}
	return name, true
}

// wordScanner walks SQL text while understanding quoted strings, backtick
// identifiers and comments. It only implements what the classifier needs.
type wordScanner struct {
	b []byte
	p int
}

func newWordScanner(s string) wordScanner {
	sc := wordScanner{b: []byte(s)}
	sc.skipSeparators()
	return sc
}

func (s *wordScanner) eof() bool {
	s.skipSeparators()
	return s.p >= len(s.b)
}

func (s *wordScanner) hasMore() bool {
	return !s.eof()
}

func (s *wordScanner) peekByte() byte {
	s.skipSeparators()
	if s.p >= len(s.b) {
		return 0
	}
	return s.b[s.p]
}

func (s *wordScanner) peekByteAt(offset int) byte {
	idx := s.p + offset
	if idx < 0 || idx >= len(s.b) {
		return 0
	}
	return s.b[idx]
}

func (s *wordScanner) readByte() byte {
	if s.p >= len(s.b) {
		return 0
	}
	c := s.b[s.p]
	s.p++
	return c
}

// peekWord returns the next unquoted word or the unquoted content of a
// backtick identifier without advancing the scanner.
func (s *wordScanner) peekWord() string {
	saved := s.p
	defer func() { s.p = saved }()
	return s.readWord()
}

// readWord consumes the next word (unquoted identifier/keyword or backtick
// quoted identifier) and returns its decoded content.
func (s *wordScanner) readWord() string {
	s.skipSeparators()
	if s.p >= len(s.b) {
		return ""
	}
	if s.b[s.p] == '`' {
		return s.readBackticked()
	}
	start := s.p
	for s.p < len(s.b) && isIdentByte(s.b[s.p]) {
		s.p++
	}
	return string(s.b[start:s.p])
}

// readBareIdentifier reads a system variable target: backtick quoted or plain,
// with an optional single dot segment ("transaction_isolation").
func (s *wordScanner) readBareIdentifier() (string, bool) {
	s.skipSeparators()
	if s.p >= len(s.b) {
		return "", false
	}
	if s.b[s.p] == '`' {
		return s.readBackticked(), true
	}
	start := s.p
	for s.p < len(s.b) && isIdentByte(s.b[s.p]) {
		s.p++
	}
	if start == s.p {
		return "", false
	}
	return string(s.b[start:s.p]), true
}

func (s *wordScanner) readBackticked() string {
	if s.peekByte() != '`' {
		return ""
	}
	s.p++
	var out strings.Builder
	for s.p < len(s.b) {
		c := s.b[s.p]
		if c == '`' {
			if s.p+1 < len(s.b) && s.b[s.p+1] == '`' {
				out.WriteByte('`')
				s.p += 2
				continue
			}
			s.p++
			break
		}
		out.WriteByte(c)
		s.p++
	}
	s.skipSeparators()
	return out.String()
}

// readSimpleIdentifierTail reads the single trailing identifier used by USE.
func (s *wordScanner) readSimpleIdentifierTail() (string, bool) {
	s.skipSeparators()
	if s.p >= len(s.b) {
		return "", false
	}
	if s.b[s.p] == '`' {
		name := s.readBackticked()
		return name, name != ""
	}
	start := s.p
	for s.p < len(s.b) {
		c := s.b[s.p]
		if c == ';' || isSpaceByte(c) {
			break
		}
		s.p++
	}
	name := string(s.b[start:s.p])
	if name == "" {
		return "", false
	}
	return name, true
}

func (s *wordScanner) rest() string {
	return string(s.b[s.p:])
}

func (s *wordScanner) skipSeparators() {
	for s.p < len(s.b) {
		c := s.b[s.p]
		switch {
		case isSpaceByte(c):
			s.p++
		case c == '-' && s.p+2 < len(s.b) && s.b[s.p+1] == '-' && isSpaceByte(s.b[s.p+2]):
			for s.p < len(s.b) && s.b[s.p] != '\n' {
				s.p++
			}
		case c == '#':
			for s.p < len(s.b) && s.b[s.p] != '\n' {
				s.p++
			}
		case c == '/' && s.p+1 < len(s.b) && s.b[s.p+1] == '*':
			s.p += 2
			for s.p+1 < len(s.b) && (s.b[s.p] != '*' || s.b[s.p+1] != '/') {
				s.p++
			}
			s.p += 2
		default:
			return
		}
	}
}

// splitStatements splits text on top-level semicolons (quotes, backticks,
// comments and parentheses are respected).
func splitStatements(query string) []string {
	return splitTopLevel(query, ';')
}

// splitTopLevel splits on a delimiter outside of quoted strings, backtick
// identifiers, comments and parentheses.
func splitTopLevel(query string, delimiter byte) []string {
	var parts []string
	depth := 0
	start := 0
	b := []byte(query)
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch c {
		case '\'', '"':
			i = skipQuoted(b, i, c)
		case '`':
			i = skipQuoted(b, i, '`')
		case '-':
			if i+2 < len(b) && b[i+1] == '-' && isSpaceByte(b[i+2]) {
				for i < len(b) && b[i] != '\n' {
					i++
				}
			}
		case '#':
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case '/':
			if i+1 < len(b) && b[i+1] == '*' {
				i += 2
				for i+1 < len(b) && (b[i] != '*' || b[i+1] != '/') {
					i++
				}
				i++
			}
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if c == delimiter && depth == 0 {
				parts = append(parts, string(b[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, string(b[start:]))
	return parts
}

func skipQuoted(b []byte, start int, quote byte) int {
	for i := start + 1; i < len(b); i++ {
		if quote != '`' && b[i] == '\\' {
			i++
			continue
		}
		if b[i] == quote {
			if i+1 < len(b) && b[i+1] == quote {
				i++
				continue
			}
			return i
		}
	}
	return len(b) - 1
}

func isBlankStatement(stmt string) bool {
	s := newWordScanner(stmt)
	return !s.hasMore()
}

func isSpaceByte(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	}
	return false
}

func isIdentByte(c byte) bool {
	return c >= 'a' && c <= 'z' ||
		c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' ||
		c == '_' || c == '$'
}

func isUserVarNameByte(c byte) bool {
	return isIdentByte(c) || c == '.' || c == '#'
}

// isSimpleSQLName accepts plain unquoted charset/collation identifiers, which
// are restricted character sets in MySQL.
func isSimpleSQLName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isIdentByte(name[i]) {
			return false
		}
	}
	return true
}

// exprReplayUnsafe reports whether a SET value expression may evaluate
// differently on a freshly restored connection. It scans outside of quoted
// text and comments.
func exprReplayUnsafe(expr string) (bool, string) {
	b := []byte(expr)
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case c == '\'' || c == '"':
			i = skipQuoted(b, i, c)
		case c == '`':
			i = skipQuoted(b, i, '`')
		case c == '-' && i+2 < len(b) && b[i+1] == '-' && isSpaceByte(b[i+2]):
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case c == '#':
			for i < len(b) && b[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(b) && b[i+1] == '*':
			i += 2
			for i+1 < len(b) && (b[i] != '*' || b[i+1] != '/') {
				i++
			}
			i++
		case c == '?':
			return true, "expression uses a placeholder"
		case c == '@':
			return true, "expression references another variable"
		case isIdentStartByte(c):
			start := i
			for i < len(b) && isIdentByte(b[i]) {
				i++
			}
			word := strings.ToLower(string(b[start:i]))
			if _, ok := nonDeterministicFunctions[word]; ok {
				return true, "expression uses non-replayable function " + word
			}
			i--
		}
	}
	return false, ""
}

func isIdentStartByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '$'
}

// nonDeterministicFunctions lists functions whose result at replay time can
// differ from the result when the ledger entry was recorded.
var nonDeterministicFunctions = map[string]struct{}{
	"now":               {},
	"sysdate":           {},
	"current_timestamp": {},
	"localtime":         {},
	"localtimestamp":    {},
	"utc_timestamp":     {},
	"unix_timestamp":    {},
	"uuid":              {},
	"uuid_short":        {},
	"rand":              {},
	"current_user":      {},
	"session_user":      {},
	"system_user":       {},
	"current_role":      {},
	"connection_id":     {},
	"last_insert_id":    {},
	"found_rows":        {},
	"row_count":         {},
}
