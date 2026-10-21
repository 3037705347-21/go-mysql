package dump

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// Unlike mysqldump, Dumper is designed for parsing and syning data easily.
type Dumper struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	ExecutionPath string

	Addr     string
	User     string
	Password string
	Protocol string

	// Will override Databases
	Tables  []string
	TableDB string

	Databases []string

	Where   string
	Charset string

	IgnoreTables map[string][]string

	ExtraOptions []string

	ErrOut io.Writer

	masterDataSkipped bool
	maxAllowedPacket  int
	hexBlob           bool

	// see detectColumnStatisticsParamSupported
	isColumnStatisticsParamSupported bool

	mysqldumpVersion    string
	sourceDataSupported bool

	Logger *slog.Logger
}

func NewDumper(executionPath string, addr string, user string, password string) (*Dumper, error) {
	var path string
	var err error

	if len(executionPath) == 0 { // No explicit path set
		path, err = exec.LookPath("mysqldump")
		if err != nil {
			path, err = exec.LookPath("mariadb-dump")
			if err != nil {
				// Using a new error as `err` will only mention mariadb-dump and not mysqldump
				return nil, errors.New("not able to find mysqldump or mariadb-dump in path")
			}
		}
	} else {
		path, err = exec.LookPath(executionPath)
		if err != nil {
			return nil, err
		}
	}

	d := new(Dumper)
	d.ExecutionPath = path
	d.Addr = addr
	d.User = user
	d.Password = password
	d.Tables = make([]string, 0, 16)
	d.Databases = make([]string, 0, 16)
	d.Charset = mysql.DEFAULT_CHARSET
	d.IgnoreTables = make(map[string][]string)
	d.ExtraOptions = make([]string, 0, 5)
	d.masterDataSkipped = false

	out, err := exec.Command(d.ExecutionPath, `--help`).CombinedOutput()
	if err != nil {
		return d, err
	}
	d.isColumnStatisticsParamSupported = d.detectColumnStatisticsParamSupported(out)
	d.mysqldumpVersion = d.getMysqldumpVersion(out)
	d.sourceDataSupported = d.detectSourceDataSupported(d.mysqldumpVersion)

	d.ErrOut = os.Stderr

	d.Logger = slog.Default()

	return d, nil
}

// New mysqldump versions try to send queries to information_schema.COLUMN_STATISTICS table which does not exist in old MySQL (<5.x).
// And we got error: "Unknown table 'COLUMN_STATISTICS' in information_schema (1109)".
//
// mysqldump may not send this query if it is started with parameter --column-statistics.
// But this parameter exists only for versions >=8.0.2 (https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-2.html).
//
// For environments where the version of mysql-server and mysqldump differs, we try to check this parameter and use it if available.
func (d *Dumper) detectColumnStatisticsParamSupported(helpOutput []byte) bool {
	return bytes.Contains(helpOutput, []byte(`--column-statistics`))
}

// mysqldump  Ver 10.19 Distrib 10.3.37-MariaDB, for linux-systemd (x86_64)`, `10.3.37-MariaDB
// opt/mysql/11.0.0/bin/mysqldump from 11.0.0-preview-MariaDB, client 10.19 for linux-systemd (x86_64)
func (d *Dumper) getMysqldumpVersion(helpOutput []byte) string {
	mysqldumpVersionRegexpNew := regexp.MustCompile(`mysqldump  Ver ([0-9][^ ]*) for`)
	if m := mysqldumpVersionRegexpNew.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	mysqldumpVersionRegexpOld := regexp.MustCompile(`mysqldump  Ver .* Distrib ([0-9][^ ]*),`)
	if m := mysqldumpVersionRegexpOld.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	mysqldumpVersionRegexpMaria := regexp.MustCompile(`mysqldump from ([0-9][^ ]*), `)
	if m := mysqldumpVersionRegexpMaria.FindSubmatch(helpOutput); m != nil {
		return string(m[1])
	}

	return ""
}

func (d *Dumper) detectSourceDataSupported(version string) bool {
	// Failed to detect mysqldump version
	if version == "" {
		return false
	}

	// MySQL 5.x
	if version[0] == byte('5') {
		return false
	}

	if strings.Contains(version, "MariaDB") {
		return false
	}

	return true
}

func (d *Dumper) SetCharset(charset string) {
	d.Charset = charset
}

func (d *Dumper) SetProtocol(protocol string) {
	d.Protocol = protocol
}

func (d *Dumper) SetWhere(where string) {
	d.Where = where
}

func (d *Dumper) SetExtraOptions(options []string) {
	d.ExtraOptions = options
}

func (d *Dumper) SetErrOut(o io.Writer) {
	d.ErrOut = o
}

// SkipMasterData: In some cloud MySQL, we have no privilege to use `--master-data`.
func (d *Dumper) SkipMasterData(v bool) {
	d.masterDataSkipped = v
}

func (d *Dumper) SetMaxAllowedPacket(i int) {
	d.maxAllowedPacket = i
}

func (d *Dumper) SetHexBlob(v bool) {
	d.hexBlob = v
}

func (d *Dumper) AddDatabases(dbs ...string) {
	d.Databases = append(d.Databases, dbs...)
}

func (d *Dumper) AddTables(db string, tables ...string) {
	if d.TableDB != db {
		d.TableDB = db
		d.Tables = d.Tables[0:0]
	}

	d.Tables = append(d.Tables, tables...)
}

func (d *Dumper) AddIgnoreTables(db string, tables ...string) {
	t := d.IgnoreTables[db]
	t = append(t, tables...)
	d.IgnoreTables[db] = t
}

func (d *Dumper) Reset() {
	d.Tables = d.Tables[0:0]
	d.TableDB = ""
	d.IgnoreTables = make(map[string][]string)
	d.Databases = d.Databases[0:0]
	d.Where = ""
}

// dumpTarget identifies a schema (when table is empty) or a single table
// to pass to mysqldump as a positional argument.
type dumpTarget struct {
	db    string
	table string
}

// configuredTargets translates the Dumper configuration into the list of
// objects a dump should cover.
func (d *Dumper) configuredTargets() []dumpTarget {
	if len(d.Tables) == 0 {
		targets := make([]dumpTarget, 0, len(d.Databases))
		for _, db := range d.Databases {
			targets = append(targets, dumpTarget{db: db})
		}
		return targets
	}

	targets := make([]dumpTarget, 0, len(d.Tables))
	for _, table := range d.Tables {
		targets = append(targets, dumpTarget{db: d.TableDB, table: table})
	}
	return targets
}

// buildArgs assembles the mysqldump command line for the given targets.
// When tables are dumped positionally, mysqldump does not emit a USE
// statement, so useDB names the database that the caller must write to the
// output stream itself.
func (d *Dumper) buildArgs(targets []dumpTarget) (args []string, useDB string) {
	args = make([]string, 0, 32)

	// Common args
	if strings.Contains(d.Addr, "/") {
		args = append(args, fmt.Sprintf("--socket=%s", d.Addr))
	} else {
		host, port, err := net.SplitHostPort(d.Addr)
		if err != nil {
			host = d.Addr
		}

		args = append(args, fmt.Sprintf("--host=%s", host))
		if port != "" {
			args = append(args, fmt.Sprintf("--port=%s", port))
		}
	}

	args = append(args, fmt.Sprintf("--user=%s", d.User))
	args = append(args, fmt.Sprintf("--password=%s", d.Password))

	if !d.masterDataSkipped {
		if d.sourceDataSupported {
			args = append(args, "--source-data")
		} else {
			args = append(args, "--master-data")
		}
	}

	if d.maxAllowedPacket > 0 {
		// mysqldump param should be --max-allowed-packet=%dM not be --max_allowed_packet=%dM
		args = append(args, fmt.Sprintf("--max-allowed-packet=%dM", d.maxAllowedPacket))
	}

	if d.Protocol != "" {
		args = append(args, fmt.Sprintf("--protocol=%s", d.Protocol))
	}

	args = append(args, "--single-transaction")
	args = append(args, "--skip-lock-tables")

	// Disable uncessary data
	args = append(args, "--compact")
	args = append(args, "--skip-opt")
	args = append(args, "--quick")

	// We only care about data
	args = append(args, "--no-create-info")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")
	args = append(args, "--skip-tz-utc")
	if d.hexBlob {
		// Use hex for the binary type
		args = append(args, "--hex-blob")
	}

	for db, tables := range d.IgnoreTables {
		for _, table := range tables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", db, table))
		}
	}

	if len(d.Charset) != 0 {
		args = append(args, fmt.Sprintf("--default-character-set=%s", d.Charset))
	}

	if len(d.Where) != 0 {
		args = append(args, fmt.Sprintf("--where=%s", d.Where))
	}

	if len(d.ExtraOptions) != 0 {
		args = append(args, d.ExtraOptions...)
	}

	if d.isColumnStatisticsParamSupported {
		args = append(args, `--column-statistics=0`)
	}

	switch {
	case len(targets) == 0:
		args = append(args, "--all-databases")
	case allTargetsDatabases(targets):
		args = append(args, "--databases")
		for _, target := range targets {
			args = append(args, target.db)
		}
	default:
		// mysqldump only accepts tables of a single database as positional
		// arguments; every table target must share the same database.
		useDB = targets[0].db
		args = append(args, targets[0].db)
		for _, target := range targets {
			args = append(args, target.table)
		}
	}

	return args, useDB
}

func allTargetsDatabases(targets []dumpTarget) bool {
	for _, target := range targets {
		if target.table != "" {
			return false
		}
	}
	return true
}

// run executes mysqldump with args, streaming its stdout to w.
func (d *Dumper) run(args []string, w io.Writer) error {
	logArgs := make([]string, len(args))
	for i, arg := range args {
		if strings.HasPrefix(arg, "--password=") {
			logArgs[i] = "--password=******"
		} else {
			logArgs[i] = arg
		}
	}
	d.Logger.Info("exec mysqldump with", slog.Any("args", logArgs))

	cmd := exec.Command(d.ExecutionPath, args...)

	cmd.Stderr = d.ErrOut
	cmd.Stdout = w

	return cmd.Run()
}

// writeUsePrefix emits the USE statement mysqldump omits when tables are
// dumped positionally.
func writeUsePrefix(w io.Writer, db string) error {
	if _, err := fmt.Fprintf(w, "USE `%s`;\n", db); err != nil {
		return fmt.Errorf(`could not write USE command: %w`, err)
	}
	return nil
}

func (d *Dumper) Dump(w io.Writer) error {
	args, useDB := d.buildArgs(d.configuredTargets())
	if useDB != "" {
		if err := writeUsePrefix(w, useDB); err != nil {
			return err
		}
	}
	return d.run(args, w)
}

// DumpTable dumps a single table with the same options configured on the
// Dumper. It is the resumable snapshot unit: callers can dump tables one by
// one and skip the ones already delivered downstream.
func (d *Dumper) DumpTable(db, table string, w io.Writer) error {
	args, useDB := d.buildArgs([]dumpTarget{{db: db, table: table}})
	if useDB != "" {
		if err := writeUsePrefix(w, useDB); err != nil {
			return err
		}
	}
	return d.run(args, w)
}

// DumpAndParse: Dump MySQL and parse immediately
func (d *Dumper) DumpAndParse(h ParseHandler) error {
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := Parse(r, h, !d.masterDataSkipped)
		_ = r.CloseWithError(err)
		done <- err
	}()

	err := d.Dump(w)
	_ = w.CloseWithError(err)

	err = <-done

	return errors.Trace(err)
}

// DumpTableAndParse dumps a single table and parses its output immediately.
func (d *Dumper) DumpTableAndParse(db, table string, h ParseHandler) error {
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := Parse(r, h, !d.masterDataSkipped)
		_ = r.CloseWithError(err)
		done <- err
	}()

	err := d.DumpTable(db, table, w)
	_ = w.CloseWithError(err)

	err = <-done

	return errors.Trace(err)
}
