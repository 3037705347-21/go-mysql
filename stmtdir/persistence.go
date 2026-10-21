package stmtdir

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"
)

const (
	dirVersion = 1

	// backupSuffix is appended to the primary path to locate the previous
	// complete directory.
	backupSuffix = ".bak"

	dirFilePerm = 0o600
	dirPerm     = 0o755
)

var safeSessionID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// fileStore manages the primary directory file and its one-generation backup.
type fileStore struct {
	primary string
	backup  string
}

func newFileStore(primary, backup string) *fileStore {
	return &fileStore{primary: primary, backup: backup}
}

// envelope is the on-disk container. Content is canonical JSON encoded as
// base64 and protected by Checksum, so any truncation, bit flip or tampering
// is detected before a single entry is trusted.
type envelope struct {
	Version  int    `json:"version"`
	Checksum string `json:"checksum"`
	Content  string `json:"content"`
}

type payload struct {
	SessionID string           `json:"session_id"`
	Entries   []persistedEntry `json:"entries"`
}

type persistedEntry struct {
	ID              string    `json:"id"`
	SQL             string    `json:"sql"`
	Params          int       `json:"params"`
	Columns         int       `json:"columns"`
	ParamSignature  string    `json:"param_signature"`
	ResultSignature string    `json:"result_signature"`
	RawParamFields  []string  `json:"raw_param_fields,omitempty"`
	RawColumnFields []string  `json:"raw_column_fields,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

func defaultPrimaryPath(dir, sessionID string) (string, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", fmt.Errorf("stmtdir: create directory %q: %w", dir, err)
	}
	name := sessionID
	if !safeSessionID.MatchString(name) {
		sum := sha256.Sum256([]byte(sessionID))
		name = "session-" + hex.EncodeToString(sum[:16])
	}
	return filepath.Join(dir, name+".stmtdir.json"), nil
}

// load reads and verifies the directory. When the primary file is unusable it
// falls back to the backup of the previous complete directory and restores the
// primary from it; the backup is preserved. If neither file verifies and the
// primary existed, ErrDirectoryCorrupt is returned without touching either
// file. Missing both files means a fresh, empty directory.
func (s *fileStore) load(ctx context.Context) (*payload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	primaryData, primaryErr := os.ReadFile(s.primary)
	if primaryErr == nil {
		p, err := decodeEnvelope(primaryData)
		if err == nil {
			return p, nil
		}
		// Corrupt primary: fall through to the backup, remembering why.
		primaryErr = err
	} else if !errors.Is(primaryErr, fs.ErrNotExist) {
		return nil, primaryErr
	}

	backupData, backupErr := os.ReadFile(s.backup)
	if backupErr != nil {
		if errors.Is(backupErr, fs.ErrNotExist) {
			if primaryErr == nil {
				return nil, nil
			}
			if errors.Is(primaryErr, fs.ErrNotExist) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: primary directory unreadable (%v) and no backup available",
				ErrDirectoryCorrupt, primaryErr)
		}
		return nil, backupErr
	}

	p, err := decodeEnvelope(backupData)
	if err != nil {
		return nil, fmt.Errorf("%w: primary (%v) and backup (%v) both fail verification",
			ErrDirectoryCorrupt, primaryErr, err)
	}

	// Restore the primary from the verified backup without consuming the
	// backup itself.
	if err := atomicWriteFile(s.primary, backupData, dirFilePerm); err != nil {
		return nil, fmt.Errorf("stmtdir: restore directory from backup: %w", err)
	}
	return p, nil
}

// save replaces the primary file atomically and retains the previous complete
// directory as the backup. A corrupt primary never replaces a valid backup.
func (s *fileStore) save(raw []byte) error {
	// Never persist an unverifiable directory.
	if _, err := decodeEnvelope(raw); err != nil {
		return fmt.Errorf("stmtdir: refusing to persist corrupt directory: %w", err)
	}

	switch info, err := os.Stat(s.primary); {
	case err == nil:
		if info.IsDir() {
			return fmt.Errorf("stmtdir: %q is a directory", s.primary)
		}
		current, readErr := os.ReadFile(s.primary)
		_, primaryErr := decodeEnvelope(current)
		switch {
		case readErr == nil && primaryErr == nil:
			// Primary is a complete directory: keep it as the backup before
			// replacing it.
			if err := os.Rename(s.primary, s.backup); err != nil {
				return fmt.Errorf("stmtdir: rotate previous directory: %w", err)
			}
		default:
			// Primary is corrupt: preserve whatever backup exists.
			if err := os.Remove(s.primary); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("stmtdir: remove corrupt directory: %w", err)
			}
		}
	case errors.Is(err, fs.ErrNotExist):
		// First save: nothing to retain.
	default:
		return err
	}

	return atomicWriteFile(s.primary, raw, dirFilePerm)
}

// atomicWriteFile writes data to a temp file in the same directory, fsyncs it
// and renames it into place, so readers never observe a partial directory.
func atomicWriteFile(path string, data []byte, perm fs.FileMode) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	// Best effort: make the rename durable.
	if d, openErr := os.Open(dir); openErr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// encodePayload serializes a snapshot of the entry map into a verified
// envelope. Entries are ordered by logical id so equal directories produce
// identical bytes.
func encodePayload(sessionID string, entries map[LogicalID]*binding) ([]byte, error) {
	ids := make([]LogicalID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	persisted := make([]persistedEntry, 0, len(entries))
	for _, id := range ids {
		b := entries[id]
		persisted = append(persisted, persistedEntry{
			ID:              string(b.ID),
			SQL:             b.SQL,
			Params:          b.Params,
			Columns:         b.Columns,
			ParamSignature:  b.ParamSignature,
			ResultSignature: b.ResultSignature,
			RawParamFields:  encodeFieldList(b.RawParamFields),
			RawColumnFields: encodeFieldList(b.RawColumnFields),
			CreatedAt:       b.CreatedAt,
		})
	}

	content, err := json.Marshal(payload{SessionID: sessionID, Entries: persisted})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(content)
	env := envelope{
		Version:  dirVersion,
		Checksum: hex.EncodeToString(sum[:]),
		Content:  base64.StdEncoding.EncodeToString(content),
	}
	raw, err := json.MarshalIndent(&env, "", "  ")
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// decodeEnvelope verifies the checksum and every entry signature before
// returning the payload. Any failure means corruption.
func decodeEnvelope(raw []byte) (*payload, error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("parse envelope: %w", err)
	}
	if env.Version != dirVersion {
		return nil, fmt.Errorf("unsupported directory version %d", env.Version)
	}
	content, err := base64.StdEncoding.DecodeString(env.Content)
	if err != nil {
		return nil, errors.New("decode directory content")
	}
	sum := sha256.Sum256(content)
	expected, err := hex.DecodeString(env.Checksum)
	if err != nil {
		return nil, errors.New("decode directory checksum")
	}
	if subtle.ConstantTimeCompare(sum[:], expected) != 1 {
		return nil, errors.New("directory checksum mismatch")
	}

	var p payload
	if err := json.Unmarshal(content, &p); err != nil {
		return nil, fmt.Errorf("parse directory content: %w", err)
	}

	seen := make(map[LogicalID]struct{}, len(p.Entries))
	for i := range p.Entries {
		e := &p.Entries[i]
		id := LogicalID(e.ID)
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("duplicate statement id %s", id)
		}
		seen[id] = struct{}{}

		params, err := decodeFieldList(e.RawParamFields)
		if err != nil {
			return nil, fmt.Errorf("statement %s: corrupt parameter fields: %w", id, err)
		}
		columns, err := decodeFieldList(e.RawColumnFields)
		if err != nil {
			return nil, fmt.Errorf("statement %s: corrupt result fields: %w", id, err)
		}
		if e.Params != len(params) || e.Columns != len(columns) {
			return nil, fmt.Errorf("statement %s: field counts do not match signature", id)
		}
		gotID, gotParamSig, gotResultSig := computeIdentity(e.SQL, params, columns)
		if gotID != id || gotParamSig != e.ParamSignature || gotResultSig != e.ResultSignature {
			return nil, fmt.Errorf("statement %s: signature does not match content", id)
		}
	}
	return &p, nil
}

func (p *payload) toBindings() (map[LogicalID]*binding, error) {
	out := make(map[LogicalID]*binding, len(p.Entries))
	for i := range p.Entries {
		e := &p.Entries[i]
		params, err := decodeFieldList(e.RawParamFields)
		if err != nil {
			return nil, err
		}
		columns, err := decodeFieldList(e.RawColumnFields)
		if err != nil {
			return nil, err
		}
		out[LogicalID(e.ID)] = &binding{
			Entry: Entry{
				ID:              LogicalID(e.ID),
				SQL:             e.SQL,
				Params:          e.Params,
				Columns:         e.Columns,
				ParamSignature:  e.ParamSignature,
				ResultSignature: e.ResultSignature,
				RawParamFields:  params,
				RawColumnFields: columns,
				CreatedAt:       e.CreatedAt,
			},
		}
	}
	return out, nil
}

func encodeFieldList(fields [][]byte) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = base64.StdEncoding.EncodeToString(f)
	}
	return out
}

func decodeFieldList(fields []string) ([][]byte, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(fields))
	for i, f := range fields {
		b, err := base64.StdEncoding.DecodeString(f)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}
