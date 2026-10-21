package canal_test

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
)

// canalfakeSource is the replication-command subset canal needs: binlog
// format check, switchable server_uuid and immediately-closing GTID dumps.
type canalfakeSource struct {
	server.EmptyHandler

	mu   sync.Mutex
	uuid string
}

func (h *canalfakeSource) setUUID(u string) {
	h.mu.Lock()
	h.uuid = u
	h.mu.Unlock()
}

func (h *canalfakeSource) getUUID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.uuid
}

func resultset(names []string, values ...[]any) *mysql.Result {
	rs, err := mysql.BuildSimpleResultset(names, values, false)
	if err != nil {
		panic(err)
	}
	return mysql.NewResult(rs)
}

func (h *canalfakeSource) HandleQuery(query string) (*mysql.Result, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	switch {
	case strings.Contains(q, "binlog_format"):
		return resultset([]string{"Variable_name", "Value"}, []any{"binlog_format", "ROW"}), nil
	case strings.Contains(q, "server_uuid"):
		return resultset([]string{"@@global.server_uuid"}, []any{h.getUUID()}), nil
	case strings.Contains(q, "binlog_checksum"):
		return resultset([]string{"Variable_name", "Value"}, []any{"BINLOG_CHECKSUM", "NONE"}), nil
	default:
		// SET NAMES / @slave_uuid / KILL ... -> OK
		return mysql.NewResultReserveResultset(0), nil
	}
}

func (h *canalfakeSource) HandleRegisterSlave([]byte) error { return nil }

func (h *canalfakeSource) HandleBinlogDump(mysql.Position) (*replication.BinlogStreamer, error) {
	return nil, fmt.Errorf("not used")
}

func (h *canalfakeSource) HandleBinlogDumpGTID(*mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()
	s.AddErrorToStreamer(io.EOF)
	return s, nil
}

func startCanalFake(t *testing.T, h *canalfakeSource) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, test_keys.RSAKey(), nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, auth.AddUser("root", "secret"))
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				co, err := srv.NewCustomizedConn(conn, auth, h)
				if err != nil {
					conn.Close()
					return
				}
				for {
					if err := co.HandleCommand(); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return l.Addr().String(), func() { l.Close() }
}

// TestCanalSourceAttributionFileGating drives canal against the fake primary
// through the SourceAttributionFile option: adopt -> quarantine on switchover
// -> explicit takeover -> resume.
func TestCanalSourceAttributionFileGating(t *testing.T) {
	h := &canalfakeSource{uuid: fakeCanalUUIDA}
	addr, cleanup := startCanalFake(t, h)
	defer cleanup()

	rosterFile := filepath.Join(t.TempDir(), "source-roster.json")

	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = "root"
	cfg.Password = "secret"
	cfg.Flavor = mysql.MySQLFlavor
	cfg.Dump.ExecutionPath = ""
	cfg.SourceAttributionFile = rosterFile
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	c, err := canal.NewCanal(cfg)
	require.NoError(t, err)
	roster := c.SourceRoster()
	require.NotNil(t, roster)

	gset, err := mysql.ParseMysqlGTIDSet(fakeCanalUUIDA + ":1-2")
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() {
		runErr <- c.StartFromGTID(gset)
	}()

	// First run is adopted and keeps streaming across disconnect retries.
	require.Eventually(t, func() bool {
		return roster.State() == replication.SourceStateTrusted
	}, 3*time.Second, 20*time.Millisecond)

	// Primary swap behind the same address: Run must return the quarantine
	// error instead of merging the new source.
	h.setUUID(fakeCanalUUIDB)
	select {
	case err := <-runErr:
		require.Error(t, err)
		require.ErrorIs(t, err, replication.ErrUnknownSource)
	case <-time.After(6 * time.Second):
		t.Fatal("canal did not stop after the source identity changed")
	}
	require.Equal(t, replication.SourceStateQuarantined, roster.State())
	pending, ok := roster.Pending()
	require.True(t, ok)
	require.Equal(t, fakeCanalUUIDB, pending.Identity.ServerUUID)

	// Explicit takeover trusts the parked instance and records the chain.
	require.NoError(t, c.TakeoverGTIDSource())
	require.Equal(t, replication.SourceStateTrusted, roster.State())
	require.Len(t, roster.Attribution().History, 1)
	require.Equal(t, fakeCanalUUIDA, roster.Attribution().History[0].Identity.ServerUUID)
	c.Close()

	// Resume like a restarted process: a brand new Canal over the same
	// attribution file is admitted to the now-trusted instance and keeps
	// streaming instead of quarantining again.
	cfg2 := canal.NewDefaultConfig()
	cfg2.Addr = addr
	cfg2.User = "root"
	cfg2.Password = "secret"
	cfg2.Flavor = mysql.MySQLFlavor
	cfg2.Dump.ExecutionPath = ""
	cfg2.SourceAttributionFile = rosterFile
	cfg2.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	c2, err := canal.NewCanal(cfg2)
	require.NoError(t, err)
	require.Equal(t, replication.SourceStateTrusted, c2.SourceRoster().State())

	resumed := make(chan error, 1)
	go func() {
		resumed <- c2.StartFromGTID(gset)
	}()
	select {
	case err := <-resumed:
		t.Fatalf("canal failed to resume after takeover: %v", err)
	case <-time.After(2500 * time.Millisecond):
	}
	c2.Close()
}

// TestCanalTakeoverWithoutRoster returns a clear error when gating is off.
func TestCanalTakeoverWithoutRoster(t *testing.T) {
	h := &canalfakeSource{uuid: fakeCanalUUIDA}
	addr, cleanup := startCanalFake(t, h)
	defer cleanup()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = "root"
	cfg.Password = "secret"
	cfg.Flavor = mysql.MySQLFlavor
	cfg.Dump.ExecutionPath = ""
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	c, err := canal.NewCanal(cfg)
	require.NoError(t, err)
	defer c.Close()
	require.Nil(t, c.SourceRoster())
	require.Error(t, c.TakeoverGTIDSource())
}

const (
	fakeCanalUUIDA = "aaaaaaaa-1111-1111-1111-111111111111"
	fakeCanalUUIDB = "bbbbbbbb-2222-2222-2222-222222222222"
)
