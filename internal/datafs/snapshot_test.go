package datafs

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/hairyhenderson/go-fsimpl/httpfs"
	"github.com/hairyhenderson/gomplate/v5/internal/config"
	"github.com/hairyhenderson/gomplate/v5/internal/iohelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotTestFS() fsWithBase {
	base := "/tmp/"
	if runtime.GOOS == osWindows {
		base = "C:/tmp/"
	}
	fsys := WrapWdFS(fstest.MapFS{
		"tmp/data.json":  &fstest.MapFile{Data: []byte(`{"foo":"bar"}`)},
		"tmp/empty":      &fstest.MapFile{Data: nil},
		"tmp/dir/a.yaml": &fstest.MapFile{Data: []byte("foo: bar\n")},
		"tmp/dir/b.yaml": &fstest.MapFile{Data: []byte("baz: qux\n")},
	})
	return fsWithBase{fsys: fsys, base: base}
}

type fsWithBase struct {
	fsys fs.FS
	base string
}

func newSnapshotTestReader(t *testing.T, f fsWithBase, alias, uPath string, hdr http.Header) (context.Context, *dsReader) {
	t.Helper()
	ctx := ContextWithFSProvider(context.Background(), WrappedFSProvider(f.fsys, "file", ""))
	reg := NewRegistry()
	reg.Register(alias, config.DataSource{
		// construct directly, like the other reader tests - url.Parse treats
		// the drive letter as a host on Windows
		URL:    &url.URL{Scheme: "file", Path: uPath},
		Header: hdr,
	})
	return ctx, &dsReader{Registry: reg}
}

func fileURL(f fsWithBase, p string) string {
	return (&url.URL{Scheme: "file", Path: f.base + p}).String()
}

// recordReplay performs a recorded read, round-trips the snapshot through
// JSON, and returns a replayer backed by the serialized snapshot.
func recordReplay(t *testing.T, rec *SnapshotRecorder) *SnapshotReplayer {
	t.Helper()
	var buf bytes.Buffer
	_, err := rec.WriteTo(&buf)
	require.NoError(t, err)

	s, err := ReadSnapshot(&buf)
	require.NoError(t, err)
	return NewSnapshotReplayer(s)
}

func TestSnapshot_RecordAndReplay(t *testing.T) {
	f := snapshotTestFS()

	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	rctx := ContextWithSnapshotRecorder(ctx, rec)

	ct, b, err := sr.ReadSource(rctx, "data")
	require.NoError(t, err)
	assert.Equal(t, iohelpers.JSONMimetype, ct)
	assert.JSONEq(t, `{"foo":"bar"}`, string(b))

	// the serialized snapshot has the expected shape
	var buf bytes.Buffer
	_, err = rec.WriteTo(&buf)
	require.NoError(t, err)
	var onDisk snapshotFile
	require.NoError(t, json.Unmarshal(buf.Bytes(), &onDisk))
	assert.Equal(t, SnapshotVersion, onDisk.Version)
	require.Len(t, onDisk.Requests, 1)
	assert.Equal(t, "data", onDisk.Requests[0].Alias)
	assert.Equal(t, "", onDisk.Requests[0].Arg)
	assert.Equal(t, fileURL(f, "data.json"), onDisk.Requests[0].URL)
	assert.Equal(t, iohelpers.JSONMimetype, onDisk.Requests[0].ContentType)

	// replay with a fresh reader and a context that carries NO filesystem
	// provider - any live access would fail
	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	plctx := ContextWithSnapshotReplayer(context.Background(), rep)

	ct2, b2, err := replayReader.ReadSource(plctx, "data")
	require.NoError(t, err)
	assert.Equal(t, ct, ct2)
	assert.Equal(t, b, b2)
}

func TestSnapshot_ReplayIncludesRawText(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	_, _, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "data")
	require.NoError(t, err)

	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	ct, b, err := replayReader.ReadSource(ContextWithSnapshotReplayer(context.Background(), rep), "data")
	require.NoError(t, err)
	assert.Equal(t, iohelpers.JSONMimetype, ct)
	assert.Equal(t, `{"foo":"bar"}`, string(b))
}

func TestSnapshot_EmptyContentPreserved(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "empty", f.base+"empty", nil)

	ct, b, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "empty")
	require.NoError(t, err)
	assert.Empty(t, b)
	assert.Equal(t, iohelpers.TextMimetype, ct)

	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "empty", f.base+"empty", nil)
	ct2, b2, err := replayReader.ReadSource(ContextWithSnapshotReplayer(context.Background(), rep), "empty")
	require.NoError(t, err)
	assert.Empty(t, b2)
	assert.Equal(t, iohelpers.TextMimetype, ct2)
}

func TestSnapshot_DirectoryDatasource(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "dir", f.base+"dir/", nil)

	ct, b, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "dir")
	require.NoError(t, err)
	assert.Equal(t, iohelpers.JSONArrayMimetype, ct)
	assert.JSONEq(t, `["a.yaml","b.yaml"]`, string(b))

	// replay must not stat or enumerate the directory
	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "dir", f.base+"dir/", nil)
	ct2, b2, err := replayReader.ReadSource(ContextWithSnapshotReplayer(context.Background(), rep), "dir")
	require.NoError(t, err)
	assert.Equal(t, iohelpers.JSONArrayMimetype, ct2)
	assert.JSONEq(t, `["a.yaml","b.yaml"]`, string(b2))
}

func TestSnapshot_SubpathsAreDistinct(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "dir", f.base+"dir/", nil)
	rctx := ContextWithSnapshotRecorder(ctx, rec)

	_, _, err := sr.ReadSource(rctx, "dir", "a.yaml?type=application/yaml")
	require.NoError(t, err)
	_, _, err = sr.ReadSource(rctx, "dir", "b.yaml?type=application/yaml")
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = rec.WriteTo(&buf)
	require.NoError(t, err)
	var onDisk snapshotFile
	require.NoError(t, json.Unmarshal(buf.Bytes(), &onDisk))
	require.Len(t, onDisk.Requests, 2)

	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "dir", f.base+"dir/", nil)
	pctx := ContextWithSnapshotReplayer(context.Background(), rep)

	_, a, err := replayReader.ReadSource(pctx, "dir", "a.yaml?type=application/yaml")
	require.NoError(t, err)
	assert.Equal(t, "foo: bar\n", string(a))

	_, b2, err := replayReader.ReadSource(pctx, "dir", "b.yaml?type=application/yaml")
	require.NoError(t, err)
	assert.Equal(t, "baz: qux\n", string(b2))

	// an unrecorded subpath fails rather than falling back to the live FS
	_, _, err = replayReader.ReadSource(pctx, "dir", "missing.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not read when the snapshot was recorded")
}

func TestSnapshot_DirectURLAlias(t *testing.T) {
	// using a datasource URL directly (without an alias registration) must
	// also be recorded and replayable
	mux := http.NewServeMux()
	mux.HandleFunc("/data.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", iohelpers.JSONMimetype)
		_, _ = w.Write([]byte(`{"foo":"bar"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	fsp := fsimpl.NewMux()
	fsp.Add(httpfs.FS)
	ctx := ContextWithFSProvider(context.Background(), fsp)

	sr := &dsReader{Registry: NewRegistry()}
	rec := NewSnapshotRecorder()
	alias := srv.URL + "/data.json"

	_, b, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), alias)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, string(b))

	// stop the server and prove replay never reaches the network
	srv.Close()
	rep := recordReplay(t, rec)
	replayReader := &dsReader{Registry: NewRegistry()}
	_, bb, err := replayReader.ReadSource(ContextWithSnapshotReplayer(context.Background(), rep), alias)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, string(bb))
}

func TestSnapshot_MissingEntryAndReachability(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	_, _, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "data")
	require.NoError(t, err)

	rep := recordReplay(t, rec)

	// a different alias has no recorded read
	reg := NewRegistry()
	u, _ := url.Parse("file:///tmp/other.json")
	reg.Register("other", config.DataSource{URL: u})
	replayReader := &dsReader{Registry: reg}
	pctx := ContextWithSnapshotReplayer(context.Background(), rep)

	_, _, err = replayReader.ReadSource(pctx, "other")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "datasource replay")
	assert.Contains(t, err.Error(), `"other"`)
}

func TestSnapshot_IdentityMismatch(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	hdr := http.Header{"X-Test": []string{"one"}}
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", hdr)
	_, _, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "data")
	require.NoError(t, err)

	rep := recordReplay(t, rec)
	pctx := ContextWithSnapshotReplayer(context.Background(), rep)

	t.Run("different URL", func(t *testing.T) {
		_, replayReader := newSnapshotTestReader(t, f, "data", "file://"+f.base+"other.json", hdr)
		_, _, err := replayReader.ReadSource(pctx, "data")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "identity mismatch")
		assert.Contains(t, err.Error(), "url")
	})

	t.Run("different headers", func(t *testing.T) {
		_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json",
			http.Header{"X-Test": []string{"two"}})
		_, _, err := replayReader.ReadSource(pctx, "data")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "identity mismatch")
		assert.Contains(t, err.Error(), "headers")
	})

	t.Run("different type hint", func(t *testing.T) {
		e := rep.s.entries[snapshotKey("data", "")]
		require.Equal(t, "", e.TypeHint)
		e.TypeHint = "application/json"
		rep.s.entries[snapshotKey("data", "")] = e
		_, _, err := rep.replay("data", "", e.URL, "", hdr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "identity mismatch")
		assert.Contains(t, err.Error(), "type hint")
	})
}

func TestSnapshot_HeaderCanonicalization(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	// lowercase header name at record time
	hdr := http.Header{"x-custom": []string{"secret"}}
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", hdr)
	_, _, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "data")
	require.NoError(t, err)

	// canonical header name at replay time - must still match
	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json",
		http.Header{"X-Custom": []string{"secret"}})
	_, _, err = replayReader.ReadSource(ContextWithSnapshotReplayer(context.Background(), rep), "data")
	require.NoError(t, err)
}

func TestSnapshot_CacheSemantics(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)

	// repeated reads in one run are cached - only one entry is recorded
	rctx := ContextWithSnapshotRecorder(ctx, rec)
	for range 3 {
		_, _, err := sr.ReadSource(rctx, "data")
		require.NoError(t, err)
	}
	assert.Len(t, rec.m, 1)

	rep := recordReplay(t, rec)
	_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	pctx := ContextWithSnapshotReplayer(context.Background(), rep)
	for range 3 {
		ct, b, err := replayReader.ReadSource(pctx, "data")
		require.NoError(t, err)
		assert.Equal(t, iohelpers.JSONMimetype, ct)
		assert.JSONEq(t, `{"foo":"bar"}`, string(b))
	}
}

func TestSnapshot_FailedReadsNotRecorded(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "missing", f.base+"nope.json", nil)
	rctx := ContextWithSnapshotRecorder(ctx, rec)

	_, _, err := sr.ReadSource(rctx, "missing")
	require.Error(t, err)
	assert.Empty(t, rec.m)

	// a failed probe followed by a successful read records only the success
	_, sr2 := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	_, _, err = sr2.ReadSource(rctx, "data")
	require.NoError(t, err)
	assert.Len(t, rec.m, 1)
}

func TestSnapshot_ReplayDoesNotMutateSnapshot(t *testing.T) {
	f := snapshotTestFS()
	rec := NewSnapshotRecorder()
	ctx, sr := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	_, _, err := sr.ReadSource(ContextWithSnapshotRecorder(ctx, rec), "data")
	require.NoError(t, err)

	var before bytes.Buffer
	_, err = rec.WriteTo(&before)
	require.NoError(t, err)
	s, err := ReadSnapshot(&before)
	require.NoError(t, err)

	_, replayReader := newSnapshotTestReader(t, f, "data", f.base+"data.json", nil)
	_, b, err := replayReader.ReadSource(
		ContextWithSnapshotReplayer(context.Background(), NewSnapshotReplayer(s)), "data")
	require.NoError(t, err)
	b[0] = 'X'

	e := s.entries[snapshotKey("data", "")]
	assert.NotEqual(t, 'X', e.Body[0])
}

func TestReadSnapshot_Errors(t *testing.T) {
	for name, in := range map[string]string{
		"not JSON":          `{{not json`,
		"empty":             ``,
		"trailing garbage":  `{"version":1,"requests":[]} garbage`,
		"wrong version":     `{"version":99,"requests":[]}`,
		"missing version":   `{"requests":[]}`,
		"missing alias":     `{"version":1,"requests":[{"arg":"","contentType":"text/plain","body":""}]}`,
		"missing mime":      `{"version":1,"requests":[{"alias":"x","body":""}]}`,
		"duplicate request": `{"version":1,"requests":[{"alias":"x","contentType":"text/plain","body":""},{"alias":"x","contentType":"text/plain","body":""}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadSnapshot(strings.NewReader(in))
			require.Error(t, err)
		})
	}

	// a valid empty snapshot round-trips
	s, err := ReadSnapshot(strings.NewReader(`{"version":1,"requests":[]}`))
	require.NoError(t, err)
	assert.Empty(t, s.entries)
}
