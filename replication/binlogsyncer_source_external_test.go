package replication_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
)

const (
	fakeUUIDA = "aaaaaaaa-1111-1111-1111-111111111111"
	fakeUUIDB = "bbbbbbbb-2222-2222-2222-222222222222"
)

// fakeGTIDSource pretends to be a MySQL primary. It answers the few SET/SHOW
// queries the syncer issues, reports a switchable server_uuid and closes every
// GTID dump immediately so the syncer's internal reconnect loop is exercised.
type fakeGTIDSource struct {
	server.EmptyHandler

	mu   sync.Mutex
	uuid string
}

func (h *fakeGTIDSource) setUUID(u string) {
	h.mu.Lock()
	h.uuid = u
	h.mu.Unlock()
}

func (h *fakeGTIDSource) getUUID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.uuid
}

func (h *fakeGTIDSource) HandleQuery(query string) (*mysql.Result, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	switch {
	case strings.Contains(q, "server_uuid"):
		rs, err := mysql.BuildSimpleResultset(
			[]string{"@@global.server_uuid"},
			[][]any{{h.getUUID()}}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(rs), nil
	case strings.Contains(q, "binlog_checksum"):
		rs, err := mysql.BuildSimpleResultset(
			[]string{"Variable_name", "Value"},
			[][]any{{"BINLOG_CHECKSUM", "NONE"}}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(rs), nil
	default:
		// SET @master_binlog_checksum / @slave_uuid / KILL -> OK
		return mysql.NewResultReserveResultset(0), nil
	}
}

func (h *fakeGTIDSource) HandleRegisterSlave([]byte) error { return nil }

func (h *fakeGTIDSource) HandleBinlogDump(mysql.Position) (*replication.BinlogStreamer, error) {
	return nil, fmt.Errorf("binlog dump by position is not used in this test")
}

func (h *fakeGTIDSource) HandleBinlogDumpGTID(*mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()
	// Close the dump right away: writeBinlogEvents returns and the server
	// side disconnects, forcing the syncer into its reconnect loop.
	s.AddErrorToStreamer(io.EOF)
	return s, nil
}

type extMemStore struct {
	mu  sync.Mutex
	rec *replication.SourceAttribution
}

func (s *extMemStore) Load() (*replication.SourceAttribution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec, nil
}

func (s *extMemStore) Save(rec *replication.SourceAttribution) error {
	s.mu.Lock()
	cp := *rec
	s.rec = &cp
	s.mu.Unlock()
	return nil
}

// startFakePrimary accepts MySQL protocol connections speaking the
// replication command subset.
func startFakePrimary(t *testing.T, h *fakeGTIDSource) (string, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD, test_keys.RSAKey(), nil)
	auth := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	require.NoError(t, auth.AddUser("root", "secret"))

	stop := make(chan struct{})
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
					select {
					case <-stop:
						return
					default:
					}
					if err := co.HandleCommand(); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return l.Addr().String(), func() {
		close(stop)
		l.Close()
	}
}

// TestBinlogSyncerSourceRosterGating verifies the end-to-end switchover
// contract:
//  1. first GTID connection is adopted as trusted and persisted;
//  2. disconnect/reconnect to the same instance is transparent (existing
//     retry behaviour, no error reaches GetEvent);
//  3. a different instance behind the same address quarantines the stream with
//     ErrUnknownSource instead of merging its GTIDs;
//  4. sync stays stopped until an explicit takeover, after which a new
//     StartSyncGTID resumes against the new instance.
func TestBinlogSyncerSourceRosterGating(t *testing.T) {
	h := &fakeGTIDSource{uuid: fakeUUIDA}
	addr, cleanup := startFakePrimary(t, h)
	defer cleanup()

	store := &extMemStore{}
	roster, err := replication.NewSourceRoster(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	var port uint16
	_, err = fmt.Sscanf(portStr, "%d", &port)
	require.NoError(t, err)

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:     1001,
		Flavor:       mysql.MySQLFlavor,
		Host:         host,
		Port:         port,
		User:         "root",
		Password:     "secret",
		SourceRoster: roster,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	defer syncer.Close()

	gset, err := mysql.ParseMysqlGTIDSet(fakeUUIDA + ":1-2")
	require.NoError(t, err)

	// 1. First contact: adopted and immediately durable.
	s, err := syncer.StartSyncGTID(gset)
	require.NoError(t, err)
	require.Equal(t, replication.SourceStateTrusted, roster.State())
	require.Equal(t, fakeUUIDA, roster.Attribution().Current.ServerUUID)
	require.Equal(t, fakeUUIDA, syncer.LastSourceIdentity().ServerUUID)
	require.NotNil(t, store.rec)
	require.Equal(t, replication.SourceStateTrusted, store.rec.State)

	// 2. Same source behind the churn of disconnects: internal retries must
	//    hide the reconnects, behaviour identical to the ungated path.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	_, err = s.GetEvent(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, replication.SourceStateTrusted, roster.State())
	require.Equal(t, fakeUUIDA, syncer.LastSourceIdentity().ServerUUID)

	// 3. Primary switchover behind the very same host:port.
	h.setUUID(fakeUUIDB)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 6*time.Second)
	_, err = s.GetEvent(ctx2)
	cancel2()
	require.ErrorIs(t, err, replication.ErrUnknownSource)

	require.Equal(t, replication.SourceStateQuarantined, roster.State())
	pending, ok := roster.Pending()
	require.True(t, ok)
	require.Equal(t, fakeUUIDB, pending.Identity.ServerUUID)
	require.Equal(t, replication.QuarantineReasonUnknownSource, pending.Reason)
	require.Equal(t, fakeUUIDA, roster.Attribution().Current.ServerUUID,
		"the trusted identity must remain untouched while quarantined")
	require.Equal(t, fakeUUIDB, syncer.LastSourceIdentity().ServerUUID)

	// The decision is durable for a process restart.
	store.mu.Lock()
	require.Equal(t, replication.SourceStateQuarantined, store.rec.State)
	store.mu.Unlock()

	// 4. Explicit takeover, then resume from the same high-water set.
	require.NoError(t, roster.TakeoverPending(""))
	require.Equal(t, replication.SourceStateTrusted, roster.State())

	s2, err := syncer.StartSyncGTID(gset)
	require.NoError(t, err)
	require.Equal(t, fakeUUIDB, roster.Attribution().Current.ServerUUID)
	require.Len(t, roster.Attribution().History, 1)
	require.Equal(t, fakeUUIDA, roster.Attribution().History[0].Identity.ServerUUID)

	// Streaming is alive against the new instance (dumps keep dropping,
	// retries keep verifying the now-trusted identity).
	ctx3, cancel3 := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	_, err = s2.GetEvent(ctx3)
	cancel3()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, replication.SourceStateTrusted, roster.State())
}

// TestBinlogSyncerWithoutRosterUnchanged proves the legacy path is untouched:
// no roster configured, a mid-stream identity swap does not surface a
// quarantine error and reconnects just keep cycling.
func TestBinlogSyncerWithoutRosterUnchanged(t *testing.T) {
	h := &fakeGTIDSource{uuid: fakeUUIDA}
	addr, cleanup := startFakePrimary(t, h)
	defer cleanup()

	host, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	var port uint16
	_, err = fmt.Sscanf(portStr, "%d", &port)
	require.NoError(t, err)

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 1002,
		Flavor:   mysql.MySQLFlavor,
		Host:     host,
		Port:     port,
		User:     "root",
		Password: "secret",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	defer syncer.Close()

	gset, err := mysql.ParseMysqlGTIDSet(fakeUUIDA + ":1-2")
	require.NoError(t, err)

	s, err := syncer.StartSyncGTID(gset)
	require.NoError(t, err)

	h.setUUID(fakeUUIDB)
	// Even after the swap, GetEvent simply blocks through reconnects because
	// no roster gates the new identity.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	_, err = s.GetEvent(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
