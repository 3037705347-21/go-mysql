package client

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/stmtdir"
)

// stmtDirectoryBackend adapts an upstream *Conn to stmtdir.Backend.
type stmtDirectoryBackend struct {
	c *Conn
}

// NewStmtDirectoryBackend wraps an upstream client connection so a persistent
// statement directory can prepare statements on it on demand and release them
// again. A new backend must be installed (and the directory told to
// InvalidatePhysical) whenever the wrapped connection is replaced: prepared
// handles never outlive their physical connection.
func NewStmtDirectoryBackend(c *Conn) stmtdir.Backend {
	return &stmtDirectoryBackend{c: c}
}

// Prepare runs COM_STMT_PREPARE on the upstream connection and returns the
// physical handle together with the raw parameter and column definitions used
// for signature verification.
func (b *stmtDirectoryBackend) Prepare(_ context.Context, query string) (stmtdir.Handle, error) {
	s, err := b.c.Prepare(query)
	if err != nil {
		return stmtdir.Handle{}, err
	}
	return stmtdir.Handle{
		ID:              s.ID,
		Params:          s.Params,
		Columns:         s.Columns,
		RawParamFields:  s.RawParamFields,
		RawColumnFields: s.RawColumnFields,
	}, nil
}

// Close sends COM_STMT_CLOSE for a handle.
func (b *stmtDirectoryBackend) Close(_ context.Context, id uint32) error {
	return b.c.writeCommandUint32(mysql.COM_STMT_CLOSE, id)
}
