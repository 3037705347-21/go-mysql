package canal

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

type EventHandler interface {
	OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error
	// OnTableChanged is called when the table is created, altered, renamed or dropped.
	// You need to clear the associated data like cache with the table.
	// It will be called before OnDDL.
	OnTableChanged(header *replication.EventHeader, schema string, table string) error
	OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error
	OnRow(e *RowsEvent) error
	OnXID(header *replication.EventHeader, nextPos mysql.Position) error
	OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error
	// OnRowsQueryEvent is called when binlog_rows_query_log_events=ON for each DML query.
	// You'll get the original executed query, with comments if present.
	// It will be called before OnRow.
	OnRowsQueryEvent(e *replication.RowsQueryEvent) error
	// OnTableNotFound is called when a Rows Event references a table object
	// that no longer exists.
	OnTableNotFound(*replication.EventHeader, *replication.RowsEvent) error
	String() string
}

// SnapshotEventHandler is an optional extension of EventHandler used by the
// resumable initial snapshot (see DumpConfig.StateDir). When the registered
// EventHandler implements it, snapshot rows are delivered as whole batches
// instead of one RowsEvent per row, giving the downstream a natural unit for
// transactional bulk writes. A batch is marked durably complete only after
// OnSnapshotBatch returns, so a crash inside a batch redelivers the whole
// batch on resume: implementations must be idempotent.
type SnapshotEventHandler interface {
	// OnSnapshotTableBegin is called before the first batch of a table is
	// (re-)delivered. Completed tables are skipped on resume, so this is
	// called at most once per table across all attempts.
	OnSnapshotTableBegin(db string, table string) error
	// OnSnapshotBatch delivers one snapshot batch of INSERT rows. batch is the
	// 1-numbered batch sequence within the table; it is contiguous across
	// resumes (a resumed table continues at the first uncommitted sequence).
	OnSnapshotBatch(e *RowsEvent, batch uint64) error
	// OnSnapshotTableEnd is called after the final batch of a table is
	// persisted, marking the table fully imported.
	OnSnapshotTableEnd(db string, table string) error
}

type DummyEventHandler struct{}

func (h *DummyEventHandler) OnRotate(*replication.EventHeader, *replication.RotateEvent) error {
	return nil
}

func (h *DummyEventHandler) OnTableChanged(*replication.EventHeader, string, string) error {
	return nil
}

func (h *DummyEventHandler) OnDDL(*replication.EventHeader, mysql.Position, *replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error                               { return nil }
func (h *DummyEventHandler) OnXID(*replication.EventHeader, mysql.Position) error { return nil }
func (h *DummyEventHandler) OnGTID(*replication.EventHeader, mysql.BinlogGTIDEvent) error {
	return nil
}

func (h *DummyEventHandler) OnPosSynced(*replication.EventHeader, mysql.Position, mysql.GTIDSet, bool) error {
	return nil
}

func (h *DummyEventHandler) OnRowsQueryEvent(*replication.RowsQueryEvent) error {
	return nil
}

// OnTableNotFound is called for row events for reference tables that are not found
func (h *DummyEventHandler) OnTableNotFound(header *replication.EventHeader, e *replication.RowsEvent) error {
	return schema.ErrTableNotExist
}

func (h *DummyEventHandler) String() string { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
