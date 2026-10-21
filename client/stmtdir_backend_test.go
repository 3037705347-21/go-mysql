package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/stmt"
	"github.com/go-mysql-org/go-mysql/stmtdir"
	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/stretchr/testify/require"
)

// probeQuery is prepared against a concrete table so the parameter and result
// column definitions reported by the server are stable across prepares.
const probeQuery = "SELECT id, name FROM stmtdir_probe WHERE id = ?"

func probeExec(t *testing.T, c *Conn, entry stmtdir.Entry, physicalID uint32, id int32) *mysql.Result {
	t.Helper()
	s := &Stmt{}
	s.conn = c
	s.PreparedStmt = stmt.PreparedStmt{
		ID:              physicalID,
		Params:          entry.Params,
		Columns:         entry.Columns,
		RawParamFields:  entry.RawParamFields,
		RawColumnFields: entry.RawColumnFields,
	}
	res, err := s.Execute(id)
	require.NoError(t, err)
	return res
}

// TestStmtDirectoryBackendRebindsAcrossConnections exercises the full proxy
// path against a real MySQL server: learn signatures once, execute through the
// directory, replace the physical connection and verify the statement is
// transparently re-prepared, then confirm the directory survives a restart.
func TestStmtDirectoryBackendRebindsAcrossConnections(t *testing.T) {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)
	c, err := Connect(addr, *testUser, *testPassword, *testDB)
	if err != nil {
		t.Skipf("skipping: no MySQL at %s: %v", addr, err)
	}
	defer c.Close()

	if _, err := c.Execute("DROP TABLE IF EXISTS stmtdir_probe"); err != nil {
		t.Skipf("skipping: cannot manage probe table: %v", err)
	}
	_, err = c.Execute("CREATE TABLE stmtdir_probe (" +
		"id INT NOT NULL, name VARCHAR(32) NOT NULL, PRIMARY KEY (id))")
	require.NoError(t, err)
	_, err = c.Execute("INSERT INTO stmtdir_probe VALUES (1, 'alpha')")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = c.Execute("DROP TABLE IF EXISTS stmtdir_probe")
	})

	// Learn the logical signature from one prepare, then release that handle:
	// the directory must prepare lazily on its own.
	discovery, err := c.Prepare(probeQuery)
	require.NoError(t, err)
	rawParams := append([][]byte(nil), discovery.RawParamFields...)
	rawColumns := append([][]byte(nil), discovery.RawColumnFields...)
	require.NoError(t, discovery.Close())

	dir := t.TempDir()
	d, err := stmtdir.Open(context.Background(), "backend-it", dir,
		stmtdir.WithBackend(NewStmtDirectoryBackend(c)))
	require.NoError(t, err)

	id, err := d.Declare(probeQuery, rawParams, rawColumns)
	require.NoError(t, err)
	require.Equal(t, 0, d.Stats().Bound)

	pid1, err := d.Bind(context.Background(), id)
	require.NoError(t, err)
	require.NotZero(t, pid1)

	entry, ok := d.Entry(id)
	require.True(t, ok)
	res := probeExec(t, c, entry, pid1, 1)
	require.Equal(t, 1, res.RowNumber())
	n, err := res.GetInt(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	name, err := res.GetString(0, 1)
	require.NoError(t, err)
	require.Equal(t, "alpha", name)

	// Simulate an upstream cutover: the handle on the old connection is dead.
	c2, err := Connect(addr, *testUser, *testPassword, *testDB)
	require.NoError(t, err)
	defer c2.Close()

	d.InvalidatePhysical()
	d.SetBackend(NewStmtDirectoryBackend(c2))
	require.Equal(t, 0, d.Stats().Bound)

	pid2, err := d.Bind(context.Background(), id)
	require.NoError(t, err)
	require.NotEqual(t, pid1, pid2, "new connection assigns a fresh handle")

	res = probeExec(t, c2, entry, pid2, 1)
	require.Equal(t, 1, res.RowNumber())
	name, err = res.GetString(0, 1)
	require.NoError(t, err)
	require.Equal(t, "alpha", name)

	// CloseStmt forwards COM_STMT_CLOSE to the current connection.
	require.NoError(t, d.CloseStmt(context.Background(), id))
	require.Empty(t, d.Entries())
	require.NoError(t, d.Close())

	// Restart: the persistence-only directory remembers the closed state and
	// refuses unknown logical ids.
	d2, err := stmtdir.Open(context.Background(), "backend-it", dir)
	require.NoError(t, err)
	defer d2.Close()
	require.Empty(t, d2.Entries())
	_, err = d2.Bind(context.Background(), id)
	require.ErrorIs(t, err, stmtdir.ErrNotFound)
}
