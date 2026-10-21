package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestClassifyQuery_Use(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		schema string
		taint  bool
	}{
		{"plain", "USE db1", "db1", false},
		{"backtick", "USE `my db`", "my db", false},
		{"semicolon", "USE db1;", "db1", false},
		{"comment", "USE db1 -- switch db\n", "db1", false},
		{"block comment", "/*c*/ USE db1", "db1", false},
		{"trailing junk", "USE db1 db2", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := classifyQuery(tc.query)
			require.Equal(t, tc.taint, e.taint, e.taintReason)
			if !tc.taint {
				require.True(t, e.hasSchema)
				require.Equal(t, tc.schema, e.schema)
			}
		})
	}
}

func TestClassifyQuery_SetSystemVars(t *testing.T) {
	e := classifyQuery("SET sql_mode = 'STRICT_TRANS_TABLES'")
	require.False(t, e.taint, e.taintReason)
	require.Len(t, e.assignments, 1)
	require.Equal(t, scopeSession, e.assignments[0].scope)
	require.Equal(t, "sql_mode", e.assignments[0].name)
	require.Equal(t, "'STRICT_TRANS_TABLES'", e.assignments[0].expr)

	for _, q := range []string{
		"SET SESSION sql_select_limit = 500",
		"SET LOCAL sql_select_limit = 500",
		"SET @@sql_select_limit = 500",
		"SET @@session.sql_select_limit = 500",
		"SET @@local.sql_select_limit = 500",
		"SET sql_select_limit := 500",
	} {
		t.Run(q, func(t *testing.T) {
			e := classifyQuery(q)
			require.False(t, e.taint, e.taintReason)
			require.Len(t, e.assignments, 1)
			require.Equal(t, scopeSession, e.assignments[0].scope)
			require.Equal(t, "sql_select_limit", e.assignments[0].name)
			require.Equal(t, "500", e.assignments[0].expr)
		})
	}
}

func TestClassifyQuery_SetGlobalIgnored(t *testing.T) {
	for _, q := range []string{
		"SET GLOBAL max_connections = 200",
		"SET @@global.max_connections = 200",
		"SET PERSIST max_connections = 200",
		"SET PERSIST_ONLY max_connections = 200",
	} {
		t.Run(q, func(t *testing.T) {
			e := classifyQuery(q)
			require.False(t, e.taint, e.taintReason)
			require.Empty(t, e.assignments)
		})
	}

	e := classifyQuery("SET GLOBAL a = 1, SESSION b = 2, c = 3")
	require.False(t, e.taint, e.taintReason)
	require.Len(t, e.assignments, 2)
	require.Equal(t, "b", e.assignments[0].name)
	require.Equal(t, "c", e.assignments[1].name)
}

func TestClassifyQuery_SetUserVars(t *testing.T) {
	e := classifyQuery("SET @agent_id = 42")
	require.False(t, e.taint, e.taintReason)
	require.Len(t, e.assignments, 1)
	require.Equal(t, scopeUser, e.assignments[0].scope)
	require.Equal(t, "agent_id", e.assignments[0].name)

	e = classifyQuery("SET @a = 1, @b := 'x', @c = 'y, z'")
	require.False(t, e.taint, e.taintReason)
	require.Len(t, e.assignments, 3)
	require.Equal(t, "c", e.assignments[2].name)
	require.Equal(t, "'y, z'", e.assignments[2].expr)
}

func TestClassifyQuery_SetSplitting(t *testing.T) {
	// Commas inside function calls and quoted strings must not split assignments.
	e := classifyQuery("SET x = CONCAT('a', 'b'), y = 'p, q'")
	require.False(t, e.taint, e.taintReason)
	require.Len(t, e.assignments, 2)
	require.Equal(t, "CONCAT('a', 'b')", e.assignments[0].expr)
	require.Equal(t, "'p, q'", e.assignments[1].expr)
}

func TestClassifyQuery_SetUnsafeTaints(t *testing.T) {
	cases := []string{
		"SET sql_mode = CONCAT(@@sql_mode, ',ONLY_FULL_GROUP_BY')",
		"SET @a = @b",
		"SET x = NOW()",
		"SET x = current_timestamp",
		"SET x = UUID()",
		"SET x = ?",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			e := classifyQuery(q)
			require.True(t, e.taint, "expected taint for %q", q)
		})
	}

	// Same tokens inside quoted strings are safe.
	e := classifyQuery("SET x = 'a@b now() ?'")
	require.False(t, e.taint, e.taintReason)
}

func TestClassifyQuery_SetNames(t *testing.T) {
	e := classifyQuery("SET NAMES utf8mb4")
	require.False(t, e.taint, e.taintReason)
	require.True(t, e.charset.present)
	require.Equal(t, charsetModeNames, e.charset.mode)
	require.Equal(t, "utf8mb4", e.charset.charset)
	require.Empty(t, e.charset.collation)

	e = classifyQuery("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
	require.False(t, e.taint, e.taintReason)
	require.Equal(t, "utf8mb4_bin", e.charset.collation)

	e = classifyQuery("SET CHARACTER SET latin1")
	require.False(t, e.taint, e.taintReason)
	require.Equal(t, charsetModeCharset, e.charset.mode)
	require.Equal(t, "latin1", e.charset.charset)

	e = classifyQuery("SET CHARSET latin1")
	require.False(t, e.taint, e.taintReason)
	require.Equal(t, charsetModeCharset, e.charset.mode)

	require.True(t, classifyQuery("SET NAMES nope!").taint)
	require.True(t, classifyQuery("SET NAMES utf8mb4 COLLATE nope!").taint)
}

func TestClassifyQuery_SetSpecial(t *testing.T) {
	e := classifyQuery("SET ROLE `r1`")
	require.False(t, e.taint, e.taintReason)
	require.Equal(t, []string{"SET ROLE `r1`"}, e.roleStatements)

	// Account/global statements do not taint.
	for _, q := range []string{
		"SET PASSWORD = 'secret'",
		"SET DEFAULT ROLE NONE TO u",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
	} {
		e := classifyQuery(q)
		require.False(t, e.taint, q+": "+e.taintReason)
		require.Empty(t, e.assignments)
	}

	require.True(t, classifyQuery("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE").taint)
}

func TestClassifyQuery_Transactions(t *testing.T) {
	require.Equal(t, int8(1), classifyQuery("BEGIN").txResult)
	require.Equal(t, int8(1), classifyQuery("BEGIN WORK").txResult)
	require.Equal(t, int8(1), classifyQuery("START TRANSACTION READ ONLY").txResult)
	require.Equal(t, int8(0), classifyQuery("COMMIT").txResult)
	require.Equal(t, int8(0), classifyQuery("ROLLBACK").txResult)
	require.Equal(t, int8(0), classifyQuery("ROLLBACK WORK AND NO CHAIN").txResult)

	// ROLLBACK TO SAVEPOINT does not end the transaction.
	e := classifyQuery("ROLLBACK TO SAVEPOINT sp1")
	require.False(t, e.taint, e.taintReason)
	require.Equal(t, int8(-1), e.txResult)

	require.True(t, classifyQuery("XA START 'xid'").taint)
	require.True(t, classifyQuery("LOCK TABLES t READ").taint)
}

func TestClassifyQuery_MultiStatement(t *testing.T) {
	e := classifyQuery("USE db1; SET x = 1; SET @u = 'a'")
	require.False(t, e.taint, e.taintReason)
	require.True(t, e.hasSchema)
	require.Equal(t, "db1", e.schema)
	require.Len(t, e.assignments, 2)
	require.Equal(t, "x", e.assignments[0].name)
	require.Equal(t, scopeUser, e.assignments[1].scope)

	e = classifyQuery("SELECT 1;")
	require.False(t, e.taint, e.taintReason)
	require.False(t, e.hasSchema)
}

func TestLedgerApplyAndFold(t *testing.T) {
	l := newSessionLedger()

	e := classifyQuery("USE db1")
	l.apply(&e)
	require.True(t, l.hasSchema)
	require.Equal(t, "db1", l.schema)

	e = classifyQuery("SET NAMES latin1 COLLATE latin1_swedish_ci")
	l.apply(&e)
	require.Equal(t, "latin1", l.charset)
	require.Equal(t, "latin1_swedish_ci", l.collation)
	require.Equal(t, charsetModeNames, l.charsetMode)

	e = classifyQuery("SET sql_select_limit = 500, @who = 'me'")
	l.apply(&e)
	require.Equal(t, "500", l.sysVars["sql_select_limit"].value)
	require.False(t, l.sysVars["sql_select_limit"].literal)
	require.Equal(t, "'me'", l.userVars["who"].value)

	e = classifyQuery("SET GLOBAL max_connections = 1")
	l.apply(&e)
	_, present := l.sysVars["max_connections"]
	require.False(t, present)

	e = classifyQuery("SET x = @y")
	require.True(t, e.taint)
	l.apply(&e)
	require.True(t, l.tainted)
	require.ErrorIs(t, l.deliveryError(), ErrSessionStateUnrecoverable)
}

func TestLedgerFoldTracking(t *testing.T) {
	l := newSessionLedger()
	l.foldTracking(&mysql.SessionTrackingInfo{
		Schema: "tracked_db",
		Variables: map[string]string{
			"time_zone":            "+00:00",
			"character_set_client": "utf8mb4",
			"collation_connection": "utf8mb4_bin",
		},
	})
	require.Equal(t, "tracked_db", l.schema)
	require.Equal(t, "+", l.sysVars["time_zone"].value[:1])
	require.True(t, l.sysVars["time_zone"].literal)
	_, present := l.sysVars["character_set_client"]
	require.False(t, present)
	_, present = l.sysVars["collation_connection"]
	require.False(t, present)

	// nil-safe.
	l.foldTracking(nil)
}

func TestBuildVarStatements(t *testing.T) {
	sys := map[string]trackedVar{
		"b":         {value: "2"},
		"a":         {value: "'lit'"},
		"time_zone": {value: "+00:00", literal: true},
	}
	require.Equal(t,
		"SET SESSION `a` = 'lit', `b` = 2, `time_zone` = '+00:00'",
		buildSysVarStatement(sys, false))
	require.Equal(t,
		"SET SESSION `a` = DEFAULT, `b` = DEFAULT, `time_zone` = DEFAULT",
		buildSysVarStatement(sys, true))

	users := map[string]trackedVar{
		"b": {value: "2"},
		"a": {value: "O'Reilly", literal: true},
	}
	require.Equal(t, "SET @a = 'O''Reilly', @b = 2", buildUserVarStatement(users, false))
	require.Equal(t, "SET @a = NULL, @b = NULL", buildUserVarStatement(users, true))

	require.Empty(t, buildSysVarStatement(nil, false))
	require.Empty(t, buildUserVarStatement(nil, false))
}

func TestQuoteSQLString(t *testing.T) {
	require.Equal(t, "'abc'", quoteSQLString("abc"))
	require.Equal(t, "'O''Reilly'", quoteSQLString("O'Reilly"))
	require.Equal(t, "'a\\\\b'", quoteSQLString(`a\b`))
}
