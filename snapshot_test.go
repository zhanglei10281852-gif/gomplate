package gomplate

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hairyhenderson/gomplate/v5/internal/urlhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustFileDS(t *testing.T, path string) DataSource {
	t.Helper()
	u, err := urlhelpers.ParseSourceURL(path)
	require.NoError(t, err)
	return DataSource{URL: u}
}

// record a run and return the stdout buffer
func recordRun(t *testing.T, dir, in string, modify func(*Config)) (*Config, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	cfg := &Config{
		Input:            in,
		Stdout:           out,
		DataSourceRecord: filepath.Join(dir, "snapshot.json"),
	}
	if modify != nil {
		modify(cfg)
	}
	require.NoError(t, Run(context.Background(), cfg))
	return cfg, out
}

func replayRun(t *testing.T, dir, in string, modify func(*Config)) string {
	t.Helper()
	out := &bytes.Buffer{}
	cfg := &Config{
		Input:            in,
		Stdout:           out,
		DataSourceReplay: filepath.Join(dir, "snapshot.json"),
	}
	if modify != nil {
		modify(cfg)
	}
	require.NoError(t, Run(context.Background(), cfg))
	return out.String()
}

func TestRun_DataSourceRecordReplay(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"bar"}`), 0o644))

	tmpl := `{{ (ds "data").foo }}/{{ include "data" }}`
	cfg := func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	}

	_, out := recordRun(t, dir, tmpl, cfg)
	assert.Equal(t, `bar/{"foo":"bar"}`, out.String())

	snapPath := filepath.Join(dir, "snapshot.json")
	b, err := os.ReadFile(snapPath)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"version": 1`)
	assert.Contains(t, string(b), filepath.ToSlash(dataPath))

	// remove the live source - replay must produce identical output with no
	// live file access
	require.NoError(t, os.Remove(dataPath))
	got := replayRun(t, dir, tmpl, cfg)
	assert.Equal(t, `bar/{"foo":"bar"}`, got)
}

func TestRun_DataSourceReplayContext(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.yaml")
	require.NoError(t, os.WriteFile(dataPath, []byte("foo: bar\n"), 0o644))

	tmpl := `{{ .data.foo }}`
	cfg := func(c *Config) {
		c.Context = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	}

	_, out := recordRun(t, dir, tmpl, cfg)
	assert.Equal(t, "bar", out.String())

	require.NoError(t, os.Remove(dataPath))
	assert.Equal(t, "bar", replayRun(t, dir, tmpl, cfg))
}

func TestRun_DataSourceReplayDefineDatasource(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"bar"}`), 0o644))

	tmpl := `{{ defineDatasource "dynamic" "` + filepath.ToSlash(dataPath) + `" }}{{ (ds "dynamic").foo }}`
	_, out := recordRun(t, dir, tmpl, nil)
	assert.Equal(t, "bar", out.String())

	require.NoError(t, os.Remove(dataPath))
	assert.Equal(t, "bar", replayRun(t, dir, tmpl, nil))
}

func TestRun_DataSourceReplayDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("a: 1\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yaml"), []byte("b: 2\n"), 0o644))

	tmpl := `{{ range (ds "data") }}{{ . }} {{ end }}`
	cfg := func(c *Config) {
		// trailing separator marks a directory datasource
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dir+string(filepath.Separator))}
	}

	_, out := recordRun(t, dir, tmpl, cfg)
	assert.Contains(t, out.String(), "a.yaml")
	assert.Contains(t, out.String(), "b.yaml")

	// remove the directory entries and replay - the listing must come from
	// the snapshot
	require.NoError(t, os.Remove(filepath.Join(dir, "a.yaml")))
	require.NoError(t, os.Remove(filepath.Join(dir, "b.yaml")))
	got := replayRun(t, dir, tmpl, cfg)
	assert.Contains(t, got, "a.yaml")
	assert.Contains(t, got, "b.yaml")
}

func TestRun_DataSourceReplayReachableSemantics(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"bar"}`), 0o644))

	// the unreachable probe fails during record, but is not a fatal error
	tmpl := `{{ if datasourceReachable "missing" }}yes{{ else }}no{{ end }}:{{ (ds "data").foo }}`
	cfg := func(c *Config) {
		c.DataSources = map[string]DataSource{
			"data":    mustFileDS(t, dataPath),
			"missing": mustFileDS(t, filepath.Join(dir, "nope.json")),
		}
	}

	_, out := recordRun(t, dir, tmpl, cfg)
	assert.Equal(t, "no:bar", out.String())

	require.NoError(t, os.Remove(dataPath))
	// the missing probe must replay as false from the snapshot (no live
	// fallback), while the recorded successful read replays its payload
	assert.Equal(t, "no:bar", replayRun(t, dir, tmpl, cfg))
}

func TestRun_DataSourceReplayMissingEntry(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"bar"}`), 0o644))

	_, _ = recordRun(t, dir, `{{ (ds "data").foo }}`, func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	})
	require.NoError(t, os.Remove(dataPath))

	out := &bytes.Buffer{}
	cfg := &Config{
		Input:            `{{ (ds "unrecorded").foo }}`,
		Stdout:           out,
		DataSourceReplay: filepath.Join(dir, "snapshot.json"),
		DataSources: map[string]DataSource{
			"unrecorded": mustFileDS(t, filepath.Join(dir, "unrecorded.json")),
		},
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "datasource replay")
	assert.Contains(t, err.Error(), `"unrecorded"`)
}

func TestRun_DataSourceReplayIdentityMismatch(t *testing.T) {
	dir := t.TempDir()
	original := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(original, []byte(`{"foo":"bar"}`), 0o644))

	_, _ = recordRun(t, dir, `{{ (ds "data").foo }}`, func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, original)}
	})

	// same alias, different URL
	other := filepath.Join(dir, "other.json")
	require.NoError(t, os.WriteFile(other, []byte(`{"foo":"WRONG"}`), 0o644))
	out := &bytes.Buffer{}
	cfg := &Config{
		Input:            `{{ (ds "data").foo }}`,
		Stdout:           out,
		DataSourceReplay: filepath.Join(dir, "snapshot.json"),
		DataSources:      map[string]DataSource{"data": mustFileDS(t, other)},
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity mismatch")
	assert.Empty(t, out.String())
}

func TestRun_DataSourceRecordReplacesOldSnapshot(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	snapPath := filepath.Join(dir, "snapshot.json")

	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"one"}`), 0o644))
	_, out1 := recordRun(t, dir, `{{ (ds "data").foo }}`, func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	})
	assert.Equal(t, "one", out1.String())
	first, err := os.ReadFile(snapPath)
	require.NoError(t, err)

	// a failed run must leave the old snapshot in place and clean up temps
	bad := &Config{
		Input:            `{{ (ds "data").foo `,
		Stdout:           &bytes.Buffer{},
		DataSources:      map[string]DataSource{"data": mustFileDS(t, dataPath)},
		DataSourceRecord: snapPath,
	}
	require.Error(t, Run(context.Background(), bad))
	after, err := os.ReadFile(snapPath)
	require.NoError(t, err)
	assert.Equal(t, first, after)

	leftovers, err := filepath.Glob(filepath.Join(dir, ".#snapshot.json.*"))
	require.NoError(t, err)
	assert.Empty(t, leftovers, "unpublished temp snapshot should be cleaned up")

	// fixing the problem and re-recording fully replaces the old snapshot,
	// which a cold replay then uses
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"two"}`), 0o644))
	_, out2 := recordRun(t, dir, `{{ (ds "data").foo }}`, func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	})
	assert.Equal(t, "two", out2.String())
	second, err := os.ReadFile(snapPath)
	require.NoError(t, err)
	assert.NotEqual(t, first, second)

	require.NoError(t, os.Remove(dataPath))
	assert.Equal(t, "two", replayRun(t, dir, `{{ (ds "data").foo }}`, func(c *Config) {
		c.DataSources = map[string]DataSource{"data": mustFileDS(t, dataPath)}
	}))
}

func TestRun_DataSourceRecordSaveFailure(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data.json")
	require.NoError(t, os.WriteFile(dataPath, []byte(`{"foo":"bar"}`), 0o644))

	cfg := &Config{
		Input:            `{{ (ds "data").foo }}`,
		Stdout:           &bytes.Buffer{},
		DataSources:      map[string]DataSource{"data": mustFileDS(t, dataPath)},
		DataSourceRecord: filepath.Join(dir, "missing-dir", "snapshot.json"),
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "datasource snapshot")
}

func TestRun_DataSourceReplayCorrupt(t *testing.T) {
	dir := t.TempDir()
	for name, contents := range map[string]string{
		"corrupt":        `{not json`,
		"bad version":    `{"version":42,"requests":[]}`,
		"trailing data":  `{"version":1,"requests":[]} extra`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, "snap-"+name+".json")
			require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
			cfg := &Config{
				Input:            `{{ (ds "data").foo }}`,
				Stdout:           &bytes.Buffer{},
				DataSourceReplay: path,
				DataSources:      map[string]DataSource{"data": mustFileDS(t, filepath.Join(dir, "data.json"))},
			}
			err := Run(context.Background(), cfg)
			require.Error(t, err)
			assert.Contains(t, strings.ToLower(err.Error()), "datasource snapshot")
		})
	}
}

func TestRun_DataSourceReplayMissingFile(t *testing.T) {
	cfg := &Config{
		Input:            `foo`,
		Stdout:           &bytes.Buffer{},
		DataSourceReplay: filepath.Join(t.TempDir(), "does-not-exist.json"),
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "datasource replay snapshot")
}

func TestValidate_SnapshotMutualExclusion(t *testing.T) {
	t.Parallel()

	c := Config{
		Input:            "foo",
		DataSourceRecord: "record.json",
		DataSourceReplay: "replay.json",
	}
	err := c.validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "datasourceRecord")
	assert.Contains(t, err.Error(), "datasourceReplay")
}

func TestConfig_MergeSnapshotOptions(t *testing.T) {
	t.Parallel()

	// fields are overridden independently, following the existing
	// config-file vs commandline merge rules
	file := &Config{DataSourceRecord: "from-file.json"}
	flags := &Config{DataSourceRecord: "from-flags.json"}
	assert.Equal(t, "from-flags.json", file.MergeFrom(flags).DataSourceRecord)

	// but after merging, both options being set is rejected
	merged := (&Config{DataSourceRecord: "from-file.json"}).
		MergeFrom(&Config{DataSourceReplay: "from-flags.json"})
	assert.Equal(t, "from-file.json", merged.DataSourceRecord)
	assert.Equal(t, "from-flags.json", merged.DataSourceReplay)
	err := merged.validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one of these options is supported")
}
