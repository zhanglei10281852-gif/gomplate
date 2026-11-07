package integration

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gotest.tools/v3/fs"
)

func TestDatasources_Snapshot_FileRecordReplay(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"config.json": `{"foo": {"bar": "baz"}}`,
		}),
	)
	t.Cleanup(tmpDir.Remove)

	dataPath := tmpDir.Join("config.json")
	snapPath := tmpDir.Join("snapshot.json")
	tmpl := `{{ (datasource "config").foo.bar }}/{{ include "config" }}`

	o, e, err := cmd(t,
		"-d", "config="+dataPath,
		"--datasource-record", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, `baz/{"foo": {"bar": "baz"}}`)

	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot not published: %v", err)
	}

	// remove the source - replay must produce the same output without it
	if err := os.Remove(dataPath); err != nil {
		t.Fatal(err)
	}

	o, e, err = cmd(t,
		"-d", "config="+dataPath,
		"--datasource-replay", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, `baz/{"foo": {"bar": "baz"}}`)
}

func TestDatasources_Snapshot_MutuallyExclusive(t *testing.T) {
	o, e, err := cmd(t,
		"--datasource-record", "record.json",
		"--datasource-replay", "replay.json",
		"-i", "hello").run()
	assertFailed(t, o, e, err, "only one of these options is supported")
}

func TestDatasources_Snapshot_CorruptReplay(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"snapshot.json": "{this is not json\n",
		}),
	)
	t.Cleanup(tmpDir.Remove)

	// the bad snapshot must fail before the template renders, even though
	// the template itself needs no datasources
	o, e, err := cmd(t,
		"--datasource-replay", tmpDir.Join("snapshot.json"),
		"-i", "hello").
		withDir(tmpDir.Path()).run()
	assertFailed(t, o, e, err, "datasource snapshot")
}

func TestDatasources_Snapshot_UnsupportedVersion(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"snapshot.json": `{"version":999,"requests":[]}`,
		}),
	)
	t.Cleanup(tmpDir.Remove)

	o, e, err := cmd(t,
		"--datasource-replay", tmpDir.Join("snapshot.json"),
		"-i", "hello").run()
	assertFailed(t, o, e, err, "unsupported datasource snapshot version")
}

func TestDatasources_Snapshot_HTTPReplayOffline(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/data.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"hello":"world"}`))
	})
	srv := httptest.NewServer(mux)

	tmpDir := fs.NewDir(t, "gomplate-inttests")
	t.Cleanup(tmpDir.Remove)
	snapPath := tmpDir.Join("snapshot.json")
	tmpl := `{{ (ds "` + srv.URL + `/data.json").hello }}`

	o, e, err := cmd(t, "--datasource-record", snapPath, "-i", tmpl).run()
	assertSuccess(t, o, e, err, "world")

	// shut the server down - replay must not perform any HTTP request
	srv.Close()
	o, e, err = cmd(t, "--datasource-replay", snapPath, "-i", tmpl).run()
	assertSuccess(t, o, e, err, "world")
}

func TestDatasources_Snapshot_ReachableSemantics(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"present.json": `{"foo":"bar"}`,
		}),
	)
	t.Cleanup(tmpDir.Remove)
	snapPath := tmpDir.Join("snapshot.json")
	tmpl := `{{ if datasourceReachable "present" }}y{{ else }}n{{ end }}` +
		`{{ if datasourceReachable "absent" }}y{{ else }}n{{ end }}`

	o, e, err := cmd(t,
		"-d", "present="+tmpDir.Join("present.json"),
		"-d", "absent="+tmpDir.Join("absent.json"),
		"--datasource-record", snapPath,
		"-i", tmpl).
		withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "yn")

	// the failed probe must not be in the snapshot
	snap, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotContains(t, string(snap), `"alias": "absent"`,
		"failed reachability probe must not be recorded as a payload")

	// with the reachable source gone, replay still reports the same result,
	// proving it does not fall back to live sources
	if err := os.Remove(tmpDir.Join("present.json")); err != nil {
		t.Fatal(err)
	}
	o, e, err = cmd(t,
		"-d", "present="+tmpDir.Join("present.json"),
		"-d", "absent="+tmpDir.Join("absent.json"),
		"--datasource-replay", snapPath,
		"-i", tmpl).
		withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "yn")
}

func TestDatasources_Snapshot_Merge(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"a.yaml": "a: one\n",
			"b.yaml": "b: two\n",
		}),
	)
	t.Cleanup(tmpDir.Remove)
	snapPath := tmpDir.Join("snapshot.json")
	mergeURL := "merge:" + filepath.ToSlash(tmpDir.Join("a.yaml")) + "|" +
		filepath.ToSlash(tmpDir.Join("b.yaml"))
	tmpl := `{{ (ds "merged").a }}-{{ (ds "merged").b }}`

	o, e, err := cmd(t,
		"-d", "merged="+mergeURL,
		"--datasource-record", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, "one-two")

	if err := os.Remove(tmpDir.Join("a.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tmpDir.Join("b.yaml")); err != nil {
		t.Fatal(err)
	}

	o, e, err = cmd(t,
		"-d", "merged="+mergeURL,
		"--datasource-replay", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, "one-two")
}

func TestDatasources_Snapshot_Directory(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithDir("data",
			fs.WithFiles(map[string]string{
				"one.yaml": "value: 1\n",
				"two.yaml": "value: 2\n",
			}),
		),
	)
	t.Cleanup(tmpDir.Remove)
	snapPath := tmpDir.Join("snapshot.json")
	dirURL := tmpDir.Join("data") + string(filepath.Separator)
	tmpl := `{{ range (ds "data") }}{{ . }} {{ end }}`

	o, e, err := cmd(t,
		"-d", "data="+dirURL,
		"--datasource-record", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, "one.yaml two.yaml ")

	if err := os.RemoveAll(tmpDir.Join("data")); err != nil {
		t.Fatal(err)
	}
	o, e, err = cmd(t,
		"-d", "data="+dirURL,
		"--datasource-replay", snapPath,
		"-i", tmpl).run()
	assertSuccess(t, o, e, err, "one.yaml two.yaml ")
}

func TestDatasources_Snapshot_ConfigFile(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"config.json": `{"foo":"bar"}`,
			"record.yaml": `
datasources:
  config:
    url: config.json
datasourceRecord: snapshot.json
`,
			"replay.yaml": `
datasources:
  config:
    url: config.json
datasourceReplay: snapshot.json
`,
		}),
	)
	t.Cleanup(tmpDir.Remove)

	o, e, err := cmd(t, "--config", "record.yaml",
		"-i", `{{ (ds "config").foo }}`).withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "bar")

	if err := os.Remove(tmpDir.Join("config.json")); err != nil {
		t.Fatal(err)
	}
	o, e, err = cmd(t, "--config", "replay.yaml",
		"-i", `{{ (ds "config").foo }}`).withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "bar")
}

func TestDatasources_Snapshot_OutputMap(t *testing.T) {
	tmpDir := fs.NewDir(t, "gomplate-inttests",
		fs.WithFiles(map[string]string{
			"meta.json": `{"name":"rendered.txt"}`,
		}),
		fs.WithDir("in",
			fs.WithFiles(map[string]string{
				"t.tmpl": `hi {{ (ds "meta").name }}`,
			}),
		),
	)
	t.Cleanup(tmpDir.Remove)
	snapPath := tmpDir.Join("snapshot.json")
	args := func(flag, path string) []string {
		return []string{
			"--input-dir", "in",
			"-d", "meta=meta.json",
			"--output-map", `{{ (ds "meta").name }}`,
			flag, path,
		}
	}

	o, e, err := cmd(t, args("--datasource-record", snapPath)...).
		withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "")

	got, err := os.ReadFile(tmpDir.Join("rendered.txt"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "hi rendered.txt", string(got))

	// the datasource that names the output is gone; the output naming
	// template and body reads must come from the snapshot
	if err := os.RemoveAll(tmpDir.Join("out")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tmpDir.Join("meta.json")); err != nil {
		t.Fatal(err)
	}
	o, e, err = cmd(t, args("--datasource-replay", snapPath)...).
		withDir(tmpDir.Path()).run()
	assertSuccess(t, o, e, err, "")

	got, err = os.ReadFile(tmpDir.Join("rendered.txt"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "hi rendered.txt", string(got))
}
