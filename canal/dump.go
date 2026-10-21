package canal

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/dump"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
	"github.com/shopspring/decimal"
)

type dumpParseHandler struct {
	c    *Canal
	name string
	pos  uint64
	gset mysql.GTIDSet
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	return nil
}

func (h *dumpParseHandler) GtidSet(gtidsets string) (err error) {
	if h.gset != nil {
		err = h.gset.Update(gtidsets)
	} else {
		h.gset, err = mysql.ParseGTIDSet("mysql", gtidsets)
	}
	return err
}

func (h *dumpParseHandler) Data(db string, table string, values []string) error {
	if err := h.c.ctx.Err(); err != nil {
		return err
	}

	tableInfo, err := h.c.getDumpTableInfo(db, table)
	if err != nil {
		return errors.Trace(err)
	}
	if tableInfo == nil {
		// table is excluded or its metadata is unavailable, skip the row
		return nil
	}

	vs, err := parseDumpValues(h.c.cfg.UseDecimal, tableInfo, values)
	if err != nil {
		return err
	}

	events := newRowsEvent(tableInfo, InsertAction, [][]any{vs}, nil, nil)
	return h.c.eventHandler.OnRow(events)
}

// getDumpTableInfo resolves the table metadata for a dump row. A nil table
// (with nil error) means the row must be silently skipped.
func (c *Canal) getDumpTableInfo(db string, table string) (*schema.Table, error) {
	tableInfo, err := c.GetTable(db, table)
	if err != nil {
		e := errors.Cause(err)
		if e == ErrExcludedTable ||
			e == schema.ErrTableNotExist ||
			e == schema.ErrMissingTableMeta {
			return nil, nil
		}
		c.cfg.Logger.Error("error getting table information", slog.String("database", db), slog.String("table", table), slog.Any("error", err))
		return nil, errors.Trace(err)
	}
	return tableInfo, nil
}

// parseDumpValues converts one mysqldump VALUES row into typed canal values.
func parseDumpValues(useDecimal bool, tableInfo *schema.Table, values []string) ([]any, error) {
	vs := make([]any, len(values))

	for i, v := range values {
		if v == "NULL" {
			vs[i] = nil
		} else if v == "_binary ''" {
			vs[i] = []byte{}
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER || tableInfo.Columns[i].Type == schema.TYPE_MEDIUM_INT {
				var n any
				var err error

				if tableInfo.Columns[i].IsUnsigned {
					n, err = strconv.ParseUint(v, 10, 64)
				} else {
					n, err = strconv.ParseInt(v, 10, 64)
				}

				if err != nil {
					return nil, fmt.Errorf("parse row %v at %d error %v, int expected", values, i, err)
				}

				vs[i] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
				}
				vs[i] = f
			} else if tableInfo.Columns[i].Type == schema.TYPE_DECIMAL {
				if useDecimal {
					d, err := decimal.NewFromString(v)
					if err != nil {
						return nil, fmt.Errorf("parse row %v at %d error %v, decimal expected", values, i, err)
					}
					vs[i] = d
				} else {
					f, err := strconv.ParseFloat(v, 64)
					if err != nil {
						return nil, fmt.Errorf("parse row %v at %d error %v, float expected", values, i, err)
					}
					vs[i] = f
				}
			} else if strings.HasPrefix(v, "0x") {
				buf, err := hex.DecodeString(v[2:])
				if err != nil {
					return nil, fmt.Errorf("parse row %v at %d error %v, hex literal expected", values, i, err)
				}
				vs[i] = string(buf)
			} else {
				return nil, fmt.Errorf("parse row %v error, invalid type at %d", values, i)
			}
		} else {
			vs[i] = v[1 : len(v)-1]
		}
	}

	return vs, nil
}

func (c *Canal) AddDumpDatabases(dbs ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddDatabases(dbs...)
}

func (c *Canal) AddDumpTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddTables(db, tables...)
}

func (c *Canal) AddDumpIgnoreTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddIgnoreTables(db, tables...)
}

func (c *Canal) dump() error {
	if c.dumper == nil {
		return errors.New("mysqldump does not exist")
	}

	c.master.UpdateTimestamp(uint32(utils.Now().Unix()))

	h := &dumpParseHandler{c: c}
	// If users call StartFromGTID with empty position to start dumping with gtid,
	// we record the current gtid position before dump starts.
	//
	// See tryDump() to see when dump is skipped.
	if c.master.GTIDSet() != nil {
		gset, err := c.GetMasterGTIDSet()
		if err != nil {
			return errors.Trace(err)
		}
		h.gset = gset
	}

	if c.cfg.Dump.SkipMasterData {
		pos, err := c.GetMasterPos()
		if err != nil {
			return errors.Trace(err)
		}
		c.cfg.Logger.Info("skip master data, get current binlog position", slog.Any("position", pos))
		h.name = pos.Name
		h.pos = uint64(pos.Pos)
	}

	start := utils.Now()
	c.cfg.Logger.Info("try dump MySQL and parse")
	if err := c.dumper.DumpAndParse(h); err != nil {
		return errors.Trace(err)
	}

	pos := mysql.Position{Name: h.name, Pos: uint32(h.pos)}
	c.master.Update(pos)
	c.master.UpdateGTIDSet(h.gset)
	if err := c.eventHandler.OnPosSynced(nil, pos, c.master.GTIDSet(), true); err != nil {
		return errors.Trace(err)
	}
	var startPos fmt.Stringer = pos
	if h.gset != nil {
		c.master.UpdateGTIDSet(h.gset)
		startPos = h.gset
	}
	c.cfg.Logger.Info("dump MySQL and parse OK", slog.Duration("use", time.Since(start)), slog.String("position", startPos.String()))
	return nil
}

func (c *Canal) tryDump() error {
	pos := c.master.Position()
	gset := c.master.GTIDSet()
	if (len(pos.Name) > 0 && pos.Pos > 0) ||
		(gset != nil && gset.String() != "") {
		// we will sync with binlog name and position
		c.cfg.Logger.Info("skip dump, use last binlog replication position or GTID set", slog.String("file", pos.Name), slog.Uint64("position", uint64(pos.Pos)), slog.Any("GTID set", gset))
		return nil
	}

	if c.dumper == nil {
		c.cfg.Logger.Info("skip dump, no mysqldump")
		return nil
	}

	if c.cfg.Dump.StateDir != "" {
		return c.resumableDump()
	}

	return c.dump()
}

// canalSnapshotApplier bridges the resumable snapshot engine and the
// registered EventHandler.
type canalSnapshotApplier struct {
	c          *Canal
	batchHooks SnapshotEventHandler
}

func (a *canalSnapshotApplier) onSnapshotTableBegin(db string, table string) error {
	if a.batchHooks != nil {
		return errors.Trace(a.batchHooks.OnSnapshotTableBegin(db, table))
	}
	return nil
}

func (a *canalSnapshotApplier) onSnapshotTableEnd(db string, table string) error {
	if a.batchHooks != nil {
		return errors.Trace(a.batchHooks.OnSnapshotTableEnd(db, table))
	}
	return nil
}

func (a *canalSnapshotApplier) applySnapshotBatch(db string, table string, batch uint64, rawRows [][]string) error {
	if err := a.c.ctx.Err(); err != nil {
		return errors.Trace(err)
	}
	if len(rawRows) == 0 {
		return nil
	}

	tableInfo, err := a.c.getDumpTableInfo(db, table)
	if err != nil {
		return errors.Trace(err)
	}
	if tableInfo == nil {
		// excluded table or missing metadata: nothing to deliver, the batch is
		// still marked complete so resume does not re-stream these rows.
		return nil
	}

	rows := make([][]any, 0, len(rawRows))
	for _, values := range rawRows {
		vs, err := parseDumpValues(a.c.cfg.UseDecimal, tableInfo, values)
		if err != nil {
			return err
		}
		rows = append(rows, vs)
	}

	if a.batchHooks != nil {
		events := newRowsEvent(tableInfo, InsertAction, rows, nil, nil)
		return errors.Trace(a.batchHooks.OnSnapshotBatch(events, batch))
	}

	// Backward-compatible delivery: one OnRow call per row, as the legacy
	// single-shot dump did.
	for _, vs := range rows {
		events := newRowsEvent(tableInfo, InsertAction, [][]any{vs}, nil, nil)
		if err := a.c.eventHandler.OnRow(events); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// snapshotTableDumper streams one table through the dumper.
func (c *Canal) snapshotTableDumper() snapshotTableSource {
	return func(_ context.Context, db string, table string, h dump.ParseHandler) error {
		return errors.Trace(c.dumper.DumpTableAndParse(db, table, h))
	}
}

// systemSnapshotSchemas are never part of an automatically enumerated
// ("all databases") snapshot plan.
var systemSnapshotSchemas = map[string]struct{}{
	"information_schema": {},
	"mysql":              {},
	"performance_schema": {},
	"sys":                {},
}

func escapeIdentifier(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

func (c *Canal) isIgnoredDumpTable(db, table string) bool {
	return slices.Contains(c.dumper.IgnoreTables[db], table)
}

// buildDumpPlan enumerates the tables a snapshot must cover, honoring the
// configured tables/databases, ignore lists and include/exclude regexes.
func (c *Canal) buildDumpPlan() ([]dumpTableRef, error) {
	if len(c.dumper.Tables) > 0 {
		refs := make([]dumpTableRef, 0, len(c.dumper.Tables))
		for _, table := range c.dumper.Tables {
			if c.isIgnoredDumpTable(c.dumper.TableDB, table) {
				continue
			}
			if !c.checkTableMatch(fmt.Sprintf("%s.%s", c.dumper.TableDB, table)) {
				continue
			}
			refs = append(refs, dumpTableRef{db: c.dumper.TableDB, table: table})
		}
		return refs, nil
	}

	dbs := c.dumper.Databases
	if len(dbs) == 0 {
		rr, err := c.Execute("SHOW DATABASES")
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := 0; i < rr.RowNumber(); i++ {
			name, err := rr.GetString(i, 0)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if _, ok := systemSnapshotSchemas[name]; ok {
				continue
			}
			dbs = append(dbs, name)
		}
	}

	refs := make([]dumpTableRef, 0)
	for _, db := range dbs {
		query := fmt.Sprintf("SHOW FULL TABLES FROM `%s`", escapeIdentifier(db))
		rr, err := c.Execute(query)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := 0; i < rr.RowNumber(); i++ {
			name, err := rr.GetString(i, 0)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// second column is BASE TABLE or VIEW; dumps only carry data rows
			// for base tables.
			if tableType, _ := rr.GetString(i, 1); strings.EqualFold(tableType, "VIEW") {
				continue
			}
			if c.isIgnoredDumpTable(db, name) {
				continue
			}
			if !c.checkTableMatch(fmt.Sprintf("%s.%s", db, name)) {
				continue
			}
			refs = append(refs, dumpTableRef{db: db, table: name})
		}
	}

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].db != refs[j].db {
			return refs[i].db < refs[j].db
		}
		return refs[i].table < refs[j].table
	})
	return refs, nil
}

// resumableDump runs the per-table, per-batch resumable snapshot and commits
// the snapshot/incremental handoff. A complete handoff from a previous run
// makes it a no-op apart from seeding the binlog start point; an incomplete
// one aborts startup before any binlog sync can run.
func (c *Canal) resumableDump() error {
	c.master.UpdateTimestamp(uint32(utils.Now().Unix()))

	store, err := OpenFileHandoffStore(c.cfg.Dump.StateDir, c.cfg.Logger)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_ = store.Close()
	}()

	batchSize := c.cfg.Dump.SnapshotBatchSize
	if batchSize <= 0 {
		batchSize = defaultSnapshotBatchSize
	}

	// Capture the start point up front when the dump stream cannot embed it,
	// and in GTID setups (same policy as the legacy dump path).
	var initial handoffStartPoint
	captured := false
	if c.cfg.Dump.SkipMasterData {
		pos, err := c.GetMasterPos()
		if err != nil {
			return errors.Trace(err)
		}
		c.cfg.Logger.Info("skip master data, get current binlog position", slog.Any("position", pos))
		initial.BinlogFile = pos.Name
		initial.BinlogPos = pos.Pos
		captured = true
	}
	if gset := c.master.GTIDSet(); gset != nil && gset.String() != "" {
		current, err := c.GetMasterGTIDSet()
		if err != nil {
			return errors.Trace(err)
		}
		initial.GTIDSet = current.String()
		captured = true
	}

	plan, err := c.buildDumpPlan()
	if err != nil {
		return errors.Trace(err)
	}

	flavor := c.cfg.Flavor
	if flavor == "" {
		flavor = mysql.DEFAULT_FLAVOR
	}

	batchHooks, _ := c.eventHandler.(SnapshotEventHandler)
	engine := &snapshotEngine{
		flavor:    flavor,
		batchSize: batchSize,
		store:     store,
		source:    c.snapshotTableDumper(),
		applier:   &canalSnapshotApplier{c: c, batchHooks: batchHooks},
		log:       c.cfg.Logger,
		now:       utils.Now,
	}

	start := utils.Now()
	record, resumed, err := engine.Run(c.ctx, initial, captured, plan)
	if err != nil {
		return errors.Trace(err)
	}

	// The snapshot-complete marker and this start point were persisted
	// together. Seed binlog sync from it and force the application handler to
	// persist the position as well.
	pos := record.Start.position()
	gset, err := record.Start.gtidSet(flavor)
	if err != nil {
		return errors.Trace(err)
	}

	c.master.Update(pos)
	if gset != nil {
		c.master.UpdateGTIDSet(gset)
	}
	if err := c.eventHandler.OnPosSynced(nil, pos, c.master.GTIDSet(), true); err != nil {
		return errors.Trace(err)
	}

	var startPos fmt.Stringer = pos
	if gset != nil {
		startPos = gset
	}
	c.cfg.Logger.Info("snapshot handoff ready, binlog sync starts here",
		slog.Bool("resumed", resumed),
		slog.Duration("use", time.Since(start)),
		slog.String("position", startPos.String()))
	return nil
}
