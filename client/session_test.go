package client

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
)

type sessionTestSuite struct {
	suite.Suite
	pool               *Pool
	defaultSelectLimit string
	port               string
}

func TestSessionSuite(t *testing.T) {
	segs := strings.SplitSeq(*test_util.MysqlPort, ",")
	for seg := range segs {
		suite.Run(t, &sessionTestSuite{port: seg})
	}
}

func (s *sessionTestSuite) SetupSuite() {
	t := s.T()
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)

	admin, err := Connect(addr, *testUser, *testPassword, "")
	require.NoError(t, err)
	for _, stmt := range []string{
		"CREATE DATABASE IF NOT EXISTS " + *testDB,
		"CREATE DATABASE IF NOT EXISTS session_a",
		"CREATE DATABASE IF NOT EXISTS session_b",
		"CREATE DATABASE IF NOT EXISTS session_doomed",
	} {
		_, err := admin.Execute(stmt)
		require.NoError(t, err, stmt)
	}
	require.NoError(t, admin.Close())

	pool, err := NewPoolWithOptions(addr, *testUser, *testPassword, *testDB,
		WithPoolLimits(1, 5, 5),
	)
	require.NoError(t, err)
	s.pool = pool

	probe, err := pool.GetConn(context.Background())
	require.NoError(t, err)
	s.defaultSelectLimit = s.scalar(probe, "SELECT @@SESSION.sql_select_limit")
	require.Equal(t, mysql.DEFAULT_CHARSET, probe.GetCharset())
	pool.PutConn(probe)
}

func (s *sessionTestSuite) TearDownSuite() {
	if s.pool != nil {
		s.pool.Close()
	}
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	admin, err := Connect(addr, *testUser, *testPassword, "")
	require.NoError(s.T(), err)
	for _, name := range []string{"session_a", "session_b", "session_doomed"} {
		_, _ = admin.Execute("DROP DATABASE IF EXISTS " + name)
	}
	require.NoError(s.T(), admin.Close())
}

func (s *sessionTestSuite) scalar(c *Conn, query string) string {
	r, err := c.Execute(query)
	require.NoError(s.T(), err, query)
	require.GreaterOrEqual(s.T(), r.RowNumber(), 1, query)
	v := r.Values[0][0].Value()
	switch val := v.(type) {
	case nil:
		return "<NULL>"
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (s *sessionTestSuite) assertState(c *Conn, schema, charsetName, collation, selectLimit, userVar string) {
	t := s.T()
	require.Equal(t, schema, s.scalar(c, "SELECT DATABASE()"))
	require.Equal(t, charsetName, s.scalar(c, "SELECT @@character_set_client"))
	if collation != "" {
		require.Equal(t, collation, s.scalar(c, "SELECT @@collation_connection"))
	}
	require.Equal(t, selectLimit, s.scalar(c, "SELECT @@SESSION.sql_select_limit"))
	require.Equal(t, userVar, s.scalar(c, "SELECT COALESCE(@agent_id, '<NULL>')"))
}

func (s *sessionTestSuite) assertBaseline(c *Conn) {
	s.assertState(c, *testDB, mysql.DEFAULT_CHARSET, "", s.defaultSelectLimit, "<NULL>")
}

// TestRestoreAfterDrop switches all tracked state, discards the physical
// connection and checks that the replacement is fully restored before delivery.
func (s *sessionTestSuite) TestRestoreAfterDrop() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)

	require.NoError(s.T(), sess.UseDB("session_a"))
	require.NoError(s.T(), sess.SetNames("latin1", "latin1_swedish_ci"))
	require.NoError(s.T(), sess.SetSessionVar("sql_select_limit", "500"))
	require.NoError(s.T(), sess.SetUserVarString("agent_id", "A-001"))
	s.assertState(c, "session_a", "latin1", "latin1_swedish_ci", "500", "A-001")

	// Physical connection disappears (crash/idle eviction) without any reset.
	sess.DropConn(c)

	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	s.assertState(c2, "session_a", "latin1", "latin1_swedish_ci", "500", "A-001")
	sess.PutConn(c2)
}

// TestRestoreAfterReturn checks state replay after a normal return/re-checkout
// cycle and that connections handed back to the pool carry no session residue.
func (s *sessionTestSuite) TestRestoreAfterReturn() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.UseDB("session_b"))
	require.NoError(s.T(), sess.SetNames("latin1", "latin1_swedish_ci"))
	require.NoError(s.T(), sess.SetSessionVar("sql_select_limit", "600"))
	require.NoError(s.T(), sess.SetUserVarString("agent_id", "B-002"))
	s.assertState(c, "session_b", "latin1", "latin1_swedish_ci", "600", "B-002")
	sess.PutConn(c)

	// The physical connection now in the pool must look like pool baseline.
	raw, err := s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	s.assertBaseline(raw)
	s.pool.PutConn(raw)

	// The logical session still owns its state across the replacement.
	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	s.assertState(c2, "session_b", "latin1", "latin1_swedish_ci", "600", "B-002")
	sess.PutConn(c2)
}

// TestSessionIsolation interleaves two logical sessions and verifies that
// neither ever observes the other's database or variables.
func (s *sessionTestSuite) TestSessionIsolation() {
	sessA := s.pool.NewSession()
	defer sessA.Close()
	sessB := s.pool.NewSession()
	defer sessB.Close()

	configure := func(sess *Session, schema, limit, who string) {
		c, err := sess.GetConn(context.Background())
		require.NoError(s.T(), err)
		require.NoError(s.T(), sess.UseDB(schema))
		require.NoError(s.T(), sess.SetSessionVar("sql_select_limit", limit))
		require.NoError(s.T(), sess.SetUserVarString("agent_id", who))
		sess.PutConn(c)
	}
	configure(sessA, "session_a", "111", "AAA")
	configure(sessB, "session_b", "222", "BBB")

	for range 4 {
		ca, err := sessA.GetConn(context.Background())
		require.NoError(s.T(), err)
		s.assertState(ca, "session_a", mysql.DEFAULT_CHARSET, "", "111", "AAA")

		cb, err := sessB.GetConn(context.Background())
		require.NoError(s.T(), err)
		s.assertState(cb, "session_b", mysql.DEFAULT_CHARSET, "", "222", "BBB")

		// A stays untouched while B works on (possibly) the same physical conn.
		require.Equal(s.T(), "session_a", s.scalar(ca, "SELECT DATABASE()"))
		require.Equal(s.T(), "111", s.scalar(ca, "SELECT @@SESSION.sql_select_limit"))
		require.Equal(s.T(), "AAA", s.scalar(ca, "SELECT @agent_id"))

		sessA.PutConn(ca)
		sessB.PutConn(cb)
	}
}

// TestTaintedLedgerRefusesDelivery verifies that statements which cannot be
// replayed deterministically poison the ledger and block later checkouts.
func (s *sessionTestSuite) TestTaintedLedgerRefusesDelivery() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.SetUserVarString("agent_id", "kept"))

	// Succeeds on the running connection, but cannot be replayed identically.
	_, err = sess.Execute("SET @v = @unrelated")
	require.NoError(s.T(), err)
	require.True(s.T(), sess.State().Tainted)

	sess.PutConn(c)

	_, err = sess.GetConn(context.Background())
	require.ErrorIs(s.T(), err, ErrSessionStateUnrecoverable)

	// Explicit reset is the documented way to start with fresh state.
	require.NoError(s.T(), sess.Reset())
	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), *testDB, s.scalar(c2, "SELECT DATABASE()"))
	require.Equal(s.T(), "<NULL>", s.scalar(c2, "SELECT COALESCE(@agent_id, '<NULL>')"))
	sess.PutConn(c2)

	// Non-deterministic system variable expression is rejected up front.
	c3, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	err = sess.SetSessionVar("sql_select_limit", "@@max_connections")
	require.ErrorIs(s.T(), err, ErrSessionStateUnrecoverable)
	sess.PutConn(c3)
}

// TestMissingDatabaseRefusesDelivery drops the tracked schema and verifies the
// session refuses to hand out a connection whose USE fails server-side.
func (s *sessionTestSuite) TestMissingDatabaseRefusesDelivery() {
	admin, err := s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	_, err = admin.Execute("CREATE DATABASE IF NOT EXISTS session_doomed")
	require.NoError(s.T(), err)
	s.pool.PutConn(admin)

	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.UseDB("session_doomed"))
	sess.PutConn(c)

	admin, err = s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	_, err = admin.Execute("DROP DATABASE session_doomed")
	require.NoError(s.T(), err)
	s.pool.PutConn(admin)

	_, err = sess.GetConn(context.Background())
	require.ErrorIs(s.T(), err, ErrSessionStateUnrecoverable)
	require.Contains(s.T(), err.Error(), "1049")

	// The pool itself remains healthy.
	probe, err := s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), probe.Ping())
	s.pool.PutConn(probe)
}

// TestTransactionReplacementRefused verifies an open transaction blocks
// delivery of a different physical connection.
func (s *sessionTestSuite) TestTransactionReplacementRefused() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.Begin())
	require.True(s.T(), sess.State().InTransaction)

	// The connection dies mid-transaction: it can never be reconstructed.
	sess.DropConn(c)
	_, err = sess.GetConn(context.Background())
	require.ErrorIs(s.T(), err, ErrSessionStateUnrecoverable)

	// Caller acknowledges the loss and starts over.
	require.NoError(s.T(), sess.Reset())
	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), c2.Ping())
	sess.PutConn(c2)

	// Returning a connection while a transaction is open discards the physical
	// connection and makes the next checkout fail loudly.
	c3, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.Begin())
	sess.PutConn(c3)
	_, err = sess.GetConn(context.Background())
	require.ErrorIs(s.T(), err, ErrSessionStateUnrecoverable)
	require.NoError(s.T(), sess.Reset())

	// A completed transaction is no obstacle at all.
	c4, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.Begin())
	require.NoError(s.T(), sess.Commit())
	sess.PutConn(c4)
	c5, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	sess.PutConn(c5)
}

// TestSetCharacterSetReplay covers the SET CHARACTER SET charset mode.
func (s *sessionTestSuite) TestSetCharacterSetReplay() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.SetCharacterSet("latin1"))
	require.Equal(s.T(), "latin1", s.scalar(c, "SELECT @@character_set_client"))
	sess.DropConn(c)

	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), "latin1", s.scalar(c2, "SELECT @@character_set_client"))
	sess.PutConn(c2)

	raw, err := s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), mysql.DEFAULT_CHARSET, s.scalar(raw, "SELECT @@character_set_client"))
	s.pool.PutConn(raw)
}

// TestExecuteInterception drives state changes through raw SQL (as a proxy
// forwarding queries would) and checks replay plus quoting edge cases.
func (s *sessionTestSuite) TestExecuteInterception() {
	sess := s.pool.NewSession()
	defer sess.Close()

	c, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)

	_, err = sess.Execute("USE session_a")
	require.NoError(s.T(), err)
	_, err = sess.Execute("SET SESSION sql_mode = 'PIPES_AS_CONCAT,ANSI_QUOTES'")
	require.NoError(s.T(), err)
	_, err = sess.Execute("SET @greeting = 'hello'")
	require.NoError(s.T(), err)
	require.NoError(s.T(), sess.SetUserVarString("weird", "a'b\\c"))

	state := sess.State()
	require.Equal(s.T(), "session_a", state.Schema)
	require.Equal(s.T(), "'PIPES_AS_CONCAT,ANSI_QUOTES'", state.SessionVars["sql_mode"])
	require.Equal(s.T(), "'hello'", state.UserVars["greeting"])
	require.Equal(s.T(), "a'b\\c", state.UserVars["weird"])

	require.Equal(s.T(), "1", s.scalar(c, "SELECT FIND_IN_SET('PIPES_AS_CONCAT', @@sql_mode) > 0"))
	require.Equal(s.T(), "1", s.scalar(c, "SELECT FIND_IN_SET('ANSI_QUOTES', @@sql_mode) > 0"))
	require.Equal(s.T(), "hello", s.scalar(c, "SELECT @greeting"))
	require.Equal(s.T(), "a'b\\c", s.scalar(c, "SELECT @weird"))

	sess.DropConn(c)

	c2, err := sess.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), "session_a", s.scalar(c2, "SELECT DATABASE()"))
	require.Equal(s.T(), "1", s.scalar(c2, "SELECT FIND_IN_SET('PIPES_AS_CONCAT', @@sql_mode) > 0"))
	require.Equal(s.T(), "1", s.scalar(c2, "SELECT FIND_IN_SET('ANSI_QUOTES', @@sql_mode) > 0"))
	require.Equal(s.T(), "hello", s.scalar(c2, "SELECT @greeting"))
	require.Equal(s.T(), "a'b\\c", s.scalar(c2, "SELECT @weird"))
	sess.PutConn(c2)

	raw, err := s.pool.GetConn(context.Background())
	require.NoError(s.T(), err)
	require.Equal(s.T(), *testDB, s.scalar(raw, "SELECT DATABASE()"))
	require.Equal(s.T(), "0", s.scalar(raw, "SELECT FIND_IN_SET('PIPES_AS_CONCAT', @@sql_mode) > 0"))
	require.Equal(s.T(), "<NULL>", s.scalar(raw, "SELECT COALESCE(@greeting, '<NULL>')"))
	s.pool.PutConn(raw)
}
