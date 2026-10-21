package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store persists lease records. Implementations must be safe for concurrent
// use. The manager performs a read-modify-write for every mutation, so stores
// do not need compare-and-swap semantics, but writes must be durable before
// Save returns: admission is only committed after a successful save.
type Store interface {
	// LoadAll returns all persisted lease records. A missing backing store
	// yields no records and no error.
	LoadAll(ctx context.Context) ([]*Record, error)

	// Save upserts a single record.
	Save(ctx context.Context, record *Record) error

	// Delete removes the record for the given server id. Deleting a missing
	// record is not an error.
	Delete(ctx context.Context, serverID uint32) error
}

// MemoryStore keeps records in memory only. It is mainly useful for tests and
// for deployments where persistence across restarts is not required.
type MemoryStore struct {
	mu      sync.Mutex
	records map[uint32]*Record
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[uint32]*Record)}
}

// LoadAll implements Store.
func (s *MemoryStore) LoadAll(_ context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*Record, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, cloneRecord(r))
	}
	return out, nil
}

// Save implements Store.
func (s *MemoryStore) Save(_ context.Context, record *Record) error {
	if record == nil {
		return fmt.Errorf("%w: nil record", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.ServerID] = cloneRecord(record)
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, serverID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, serverID)
	return nil
}

// fileFormatVersion is the version of the on-disk JSON layout.
const fileFormatVersion = 1

// fileDocument is the JSON envelope persisted by FileStore.
type fileDocument struct {
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	Leases    []*Record `json:"leases"`
}

// FileStore persists all leases as a single JSON document using atomic
// temp-file-and-rename semantics, so a crashed write never leaves a partially
// written file. It is safe for concurrent use within one process; it is not
// intended to be shared by multiple processes writing concurrently.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore creates a store backed by path. Parent directories are created
// lazily on first write.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty store path", ErrInvalidArgument)
	}
	return &FileStore{path: path}, nil
}

// Path returns the configured file path.
func (s *FileStore) Path() string { return s.path }

// LoadAll implements Store.
func (s *FileStore) LoadAll(ctx context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, err := s.readLocked(ctx)
	if err != nil || doc == nil {
		return nil, err
	}
	return doc.Leases, nil
}

func (s *FileStore) readLocked(_ context.Context) (*fileDocument, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lease: read %s: %w", s.path, err)
	}

	var doc fileDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("lease: decode %s: %w", s.path, err)
	}
	if doc.Version != fileFormatVersion {
		return nil, fmt.Errorf("lease: %s: unsupported format version %d", s.path, doc.Version)
	}
	return &doc, nil
}

func (s *FileStore) writeLocked(ctx context.Context, mutate func(map[uint32]*Record)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	doc, err := s.readLocked(ctx)
	byID := make(map[uint32]*Record)
	if err != nil {
		return err
	}
	if doc != nil {
		for _, r := range doc.Leases {
			byID[r.ServerID] = r
		}
	}

	mutate(byID)

	leases := make([]*Record, 0, len(byID))
	for _, r := range byID {
		leases = append(leases, r)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].ServerID < leases[j].ServerID })

	out := fileDocument{
		Version:   fileFormatVersion,
		UpdatedAt: time.Now(),
		Leases:    leases,
	}

	return s.replaceLocked(ctx, &out)
}

func (s *FileStore) replaceLocked(ctx context.Context, doc *fileDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("lease: create directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".lease-*.tmp")
	if err != nil {
		return fmt.Errorf("lease: create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("lease: encode %s: %w", s.path, err)
	}
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("lease: write %s: %w", s.path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("lease: sync %s: %w", s.path, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("lease: close %s: %w", s.path, err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		cleanup()
		return fmt.Errorf("lease: chmod %s: %w", s.path, err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("lease: replace %s: %w", s.path, err)
	}
	return nil
}

// Save implements Store.
func (s *FileStore) Save(ctx context.Context, record *Record) error {
	if record == nil {
		return fmt.Errorf("%w: nil record", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeLocked(ctx, func(byID map[uint32]*Record) {
		byID[record.ServerID] = cloneRecord(record)
	})
}

// Delete implements Store.
func (s *FileStore) Delete(ctx context.Context, serverID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writeLocked(ctx, func(byID map[uint32]*Record) {
		delete(byID, serverID)
	})
}
