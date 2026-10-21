package dump

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingParseHandler counts parsed data rows and records their origin.
type countingParseHandler struct {
	db     string
	tables []string
	rows   int
	name   string
	pos    uint64
}

func (h *countingParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *countingParseHandler) GtidSet(string) error { return nil }

func (h *countingParseHandler) Data(db, table string, values []string) error {
	h.db = db
	h.tables = append(h.tables, table)
	h.rows++
	return nil
}

// fakeMysqldump writes a small shell script acting as mysqldump: it verifies
// it was invoked with positional db/table arguments and emits table data.
func fakeMysqldump(t *testing.T, wantDB, wantTable string, lines ...string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mysqldump")
	script := "#!/bin/sh\n" +
		"last=\"\"; prev=\"\"\n" +
		"for a in \"$@\"; do prev=\"$last\"; last=\"$a\"; done\n" +
		"if [ \"$prev\" != \"" + wantDB + "\" ] || [ \"$last\" != \"" + wantTable + "\" ]; then\n" +
		"echo \"unexpected positional args: $prev $last\" 1>&2; exit 2\n" +
		"fi\n" +
		"echo 'CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='\\''mysql-bin.000001'\\'', SOURCE_LOG_POS=154;'\n"
	for _, l := range lines {
		script += "echo '" + l + "'\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

func TestDumper_DumpTableAndParse(t *testing.T) {
	exec := fakeMysqldump(t, "shop", "orders",
		"INSERT INTO `orders` VALUES (1,'a');",
		"INSERT INTO `orders` VALUES (2,'b');",
		"INSERT INTO `orders` VALUES (3,'c');",
	)
	d := &Dumper{
		ExecutionPath: exec,
		Addr:          "127.0.0.1:3306",
		User:          "root",
		Charset:       "utf8mb4",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ErrOut:        io.Discard,
		IgnoreTables:  map[string][]string{},
	}

	h := &countingParseHandler{}
	require.NoError(t, d.DumpTableAndParse("shop", "orders", h))

	require.Equal(t, 3, h.rows, "all rows of the single table streamed")
	require.Equal(t, "shop", h.db, "db comes from the USE prefix written for positional tables")
	require.Equal(t, []string{"orders", "orders", "orders"}, h.tables)
	require.Equal(t, "mysql-bin.000001", h.name)
	require.EqualValues(t, 154, h.pos)
}

func TestDumper_DumpTableAndParse_SkipMasterData(t *testing.T) {
	exec := fakeMysqldump(t, "shop", "orders",
		"INSERT INTO `orders` VALUES (1,'a');",
	)
	d := &Dumper{
		ExecutionPath:     exec,
		Addr:              "127.0.0.1:3306",
		User:              "root",
		Charset:           "utf8mb4",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ErrOut:            io.Discard,
		IgnoreTables:      map[string][]string{},
		masterDataSkipped: true,
	}

	h := &countingParseHandler{}
	require.NoError(t, d.DumpTableAndParse("shop", "orders", h))
	require.Equal(t, 1, h.rows)
	require.Zero(t, h.pos, "coordinate line must be ignored when master data is skipped")
}
