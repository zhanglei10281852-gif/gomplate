package datafs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"sync"
)

// SnapshotVersion is the version of the datasource snapshot file format
// produced and understood by this version of gomplate.
const SnapshotVersion = 1

// snapshotEntry is a single recorded datasource read. The request is
// identified by Alias and Arg (the first read argument - the datasource
// subpath). URL, TypeHint, and Header capture the request's resolved identity
// so that replaying a run where the datasource is defined differently fails
// instead of returning the wrong data.
type snapshotEntry struct {
	Alias       string      `json:"alias"`
	Arg         string      `json:"arg"`
	URL         string      `json:"url"`
	TypeHint    string      `json:"typeHint,omitempty"`
	Header      http.Header `json:"header,omitempty"`
	ContentType string      `json:"contentType"`
	// Body holds the raw bytes of the successful read. Empty content is
	// recorded as an empty value - it must not be confused with a missing
	// entry. JSON encodes this as base64.
	Body []byte `json:"body"`
}

// snapshotFile is the on-disk representation of a datasource snapshot.
type snapshotFile struct {
	Version  int             `json:"version"`
	Requests []snapshotEntry `json:"requests"`
}

// snapshotKey identifies a logical datasource request within a run. Only the
// alias and first read argument affect what is read, mirroring the read
// cache's cacheKey.
func snapshotKey(alias, arg string) string {
	return alias + "\x00" + arg
}

// canonicalHeader returns a copy of h with canonical header names. nil is
// returned for empty headers so that empty and nil headers compare equal.
func canonicalHeader(h http.Header) http.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(http.Header, len(h))
	for k, vs := range h {
		out[http.CanonicalHeaderKey(k)] = append([]string(nil), vs...)
	}
	return out
}

func headersEqual(a, b http.Header) bool {
	return reflect.DeepEqual(canonicalHeader(a), canonicalHeader(b))
}

// SnapshotRecorder collects datasource reads that succeed during a run. It is
// safe for concurrent use.
type SnapshotRecorder struct {
	mu sync.Mutex
	m  map[string]snapshotEntry
}

// NewSnapshotRecorder returns a recorder ready to collect successful
// datasource reads.
func NewSnapshotRecorder() *SnapshotRecorder {
	return &SnapshotRecorder{m: map[string]snapshotEntry{}}
}

// record stores a single successful read, replacing any previous entry for
// the same logical request (as happens with the on-demand read cache).
func (r *SnapshotRecorder) record(alias, arg, u, hint, contentType string, hdr http.Header, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.m[snapshotKey(alias, arg)] = snapshotEntry{
		Alias:       alias,
		Arg:         arg,
		URL:         u,
		TypeHint:    hint,
		Header:      canonicalHeader(hdr),
		ContentType: contentType,
		Body:        bytes.Clone(body),
	}
}

// WriteTo writes all recorded reads, sorted by alias and subpath for stable
// output, as a snapshot file.
func (r *SnapshotRecorder) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	entries := make([]snapshotEntry, 0, len(r.m))
	for _, e := range r.m {
		entries = append(entries, e)
	}
	r.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Alias != entries[j].Alias {
			return entries[i].Alias < entries[j].Alias
		}
		return entries[i].Arg < entries[j].Arg
	})

	b, err := json.MarshalIndent(snapshotFile{
		Version:  SnapshotVersion,
		Requests: entries,
	}, "", "  ")
	if err != nil {
		return 0, err
	}
	b = append(b, '\n')

	n, err := w.Write(b)
	return int64(n), err
}

// Snapshot is a parsed collection of recorded datasource reads.
type Snapshot struct {
	entries map[string]snapshotEntry
}

// ReadSnapshot parses a snapshot file from r, failing on corrupt content or
// unsupported versions.
func ReadSnapshot(r io.Reader) (*Snapshot, error) {
	var f snapshotFile
	dec := json.NewDecoder(r)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("datasource snapshot is corrupt: %w", err)
	}
	// be strict about trailing data - the file must contain exactly one
	// snapshot document
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("datasource snapshot is corrupt: unexpected trailing data")
		}
		return nil, fmt.Errorf("datasource snapshot is corrupt: %w", err)
	}

	if f.Version != SnapshotVersion {
		return nil, fmt.Errorf("unsupported datasource snapshot version %d: only version %d is supported",
			f.Version, SnapshotVersion)
	}

	s := &Snapshot{entries: make(map[string]snapshotEntry, len(f.Requests))}
	for i, e := range f.Requests {
		if e.Alias == "" {
			return nil, fmt.Errorf("datasource snapshot is corrupt: request %d is missing an alias", i)
		}
		if e.ContentType == "" {
			return nil, fmt.Errorf("datasource snapshot is corrupt: request %d (alias %q) is missing a content type", i, e.Alias)
		}
		if _, dup := s.entries[snapshotKey(e.Alias, e.Arg)]; dup {
			return nil, fmt.Errorf("datasource snapshot is corrupt: duplicate request for alias %q (subpath %q)", e.Alias, e.Arg)
		}
		s.entries[snapshotKey(e.Alias, e.Arg)] = e
	}

	return s, nil
}

// SnapshotReplayer serves datasource reads from a recorded Snapshot, never
// falling back to live sources.
type SnapshotReplayer struct {
	s *Snapshot
}

// NewSnapshotReplayer returns a replayer for the given snapshot.
func NewSnapshotReplayer(s *Snapshot) *SnapshotReplayer {
	return &SnapshotReplayer{s: s}
}

// replay returns the recorded content for the request identified by alias and
// arg, after verifying that the request resolves to the same URL, type hint,
// and effective headers as when recorded. A missing entry or an identity
// mismatch is an explicit error - the caller must not access live sources.
func (r *SnapshotReplayer) replay(alias, arg, u, hint string, hdr http.Header) (string, []byte, error) {
	e, ok := r.s.entries[snapshotKey(alias, arg)]
	if !ok {
		if arg == "" {
			return "", nil, fmt.Errorf(
				"datasource replay: datasource %q (url %s) was not read when the snapshot was recorded; refusing to access the live datasource",
				alias, u)
		}
		return "", nil, fmt.Errorf(
			"datasource replay: datasource %q subpath %q (url %s) was not read when the snapshot was recorded; refusing to access the live datasource",
			alias, arg, u)
	}

	if e.URL != u {
		return "", nil, fmt.Errorf(
			"datasource replay: identity mismatch for datasource %q (subpath %q): recorded url %q does not match requested url %q",
			alias, arg, e.URL, u)
	}
	if e.TypeHint != hint {
		return "", nil, fmt.Errorf(
			"datasource replay: identity mismatch for datasource %q (subpath %q): recorded type hint %q does not match requested type hint %q",
			alias, arg, e.TypeHint, hint)
	}
	if !headersEqual(e.Header, hdr) {
		return "", nil, fmt.Errorf(
			"datasource replay: identity mismatch for datasource %q (subpath %q): recorded headers %v do not match requested headers %v",
			alias, arg, e.Header, canonicalHeader(hdr))
	}

	return e.ContentType, bytes.Clone(e.Body), nil
}

// snapshotState carries either a recorder or a replayer through the context.
type snapshotState struct {
	recorder *SnapshotRecorder
	replayer *SnapshotReplayer
}

type snapshotCtxKey struct{}

// ContextWithSnapshotRecorder injects a recorder into the context, causing
// all successful datasource reads made with it to be collected.
func ContextWithSnapshotRecorder(ctx context.Context, r *SnapshotRecorder) context.Context {
	return context.WithValue(ctx, snapshotCtxKey{}, &snapshotState{recorder: r})
}

// ContextWithSnapshotReplayer injects a replayer into the context, causing
// all datasource reads made with it to be served from the snapshot.
func ContextWithSnapshotReplayer(ctx context.Context, r *SnapshotReplayer) context.Context {
	return context.WithValue(ctx, snapshotCtxKey{}, &snapshotState{replayer: r})
}

func snapshotFromContext(ctx context.Context) *snapshotState {
	s, _ := ctx.Value(snapshotCtxKey{}).(*snapshotState)
	return s
}
