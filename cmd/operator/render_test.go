package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func testSource(files ...sourceFile) *unstructured.Unstructured {
	entries := make([]interface{}, 0, len(files))
	for _, file := range files {
		entries = append(entries, map[string]interface{}{
			"path": file.path, "body": file.body, "executable": file.executable,
		})
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": coalesceResource.GroupVersion().String(), "kind": coalesceKind,
		"metadata": map[string]interface{}{
			"namespace": "coalesce", "name": "hello", "uid": "source-uid", "generation": int64(1),
		},
		"spec": map[string]interface{}{"files": entries},
	}}
}

type extractedFile struct {
	body string
	mode int64
}

func unpack(t *testing.T, data []byte) map[string]extractedFile {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	files := map[string]extractedFile{}
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		files[strings.TrimPrefix(header.Name, "./")] = extractedFile{string(body), header.Mode}
	}
	return files
}

func TestArchiveDeterminismAndHeaders(t *testing.T) {
	files := []sourceFile{{"z/run", true, "#!/bin/zsh\nprint héllo\n"}, {"a file", false, "no final newline"}, {".hidden", false, ""}}
	var first, reordered bytes.Buffer
	if err := writeArchive(&first, files); err != nil {
		t.Fatal(err)
	}
	if err := writeArchive(&reordered, []sourceFile{files[2], files[1], files[0]}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), reordered.Bytes()) {
		t.Fatal("authored order changed archive bytes")
	}
	if files[0].path != "z/run" {
		t.Fatal("archiving mutated the input order")
	}
	want := map[string]extractedFile{
		"z/run": {files[0].body, 0755}, "a file": {files[1].body, 0644}, ".hidden": {"", 0644},
	}
	if got := unpack(t, first.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("extracted tree = %#v", got)
	}
	gz, err := gzip.NewReader(bytes.NewReader(first.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	if gz.OS != 255 || !gz.ModTime.IsZero() || gz.Name != "" || gz.Comment != "" || len(gz.Extra) != 0 {
		t.Fatalf("gzip header = %#v", gz.Header)
	}
	reader := tar.NewReader(gz)
	for _, name := range []string{".hidden", "a file", "z/run"} {
		header, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if header.Name != name || header.Typeflag != tar.TypeReg || header.Format != tar.FormatUSTAR ||
			header.Uid != 0 || header.Gid != 0 || header.Uname != "root" || header.Gname != "root" ||
			!header.ModTime.Equal(time.Unix(0, 0)) || !header.AccessTime.IsZero() || !header.ChangeTime.IsZero() ||
			len(header.PAXRecords) != 0 || len(header.Xattrs) != 0 || header.Size != int64(len(want[name].body)) {
			t.Fatalf("tar header = %#v", header)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected extra entry: %v", err)
	}
	for _, change := range []sourceFile{{"z/run", false, files[0].body}, {"z/run", true, strings.TrimSuffix(files[0].body, "\n")}} {
		var changed bytes.Buffer
		if err := writeArchive(&changed, []sourceFile{change, files[1], files[2]}); err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(first.Bytes(), changed.Bytes()) {
			t.Fatal("mode/content change did not change bytes")
		}
	}
}

func TestArchiveRejectsUnextractablePaths(t *testing.T) {
	cases := [][]string{
		{""}, {"."}, {".."}, {"/absolute"}, {"../escape"}, {"a/../b"}, {"./a"}, {"a//b"}, {"a/"}, {"a\x00b"},
		{"duplicate", "duplicate"}, {"a/b", "a-other", "a"}, {"é"}, {strings.Repeat("x", 101)},
	}
	for _, names := range cases {
		t.Run(strings.Join(names, ","), func(t *testing.T) {
			var files []sourceFile
			for _, name := range names {
				files = append(files, sourceFile{path: name, body: "x"})
			}
			if err := writeArchive(io.Discard, files); err == nil {
				t.Fatalf("accepted paths %q", names)
			}
		})
	}
	// USTAR can split a long path into prefix/name fields; a blanket 100-byte
	// limit would incorrectly reject this representable path.
	if err := writeArchive(io.Discard, []sourceFile{{path: strings.Repeat("p", 120) + "/run"}}); err != nil {
		t.Fatal(err)
	}
}

type failingWriter struct{ remaining int }

var errDestination = errors.New("destination failed")

func (w *failingWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		n := w.remaining
		w.remaining = 0
		return n, errDestination
	}
	w.remaining -= len(p)
	return len(p), nil
}

func TestArchivePropagatesWriteAndCloseFailures(t *testing.T) {
	for _, limit := range []int{0, 10} {
		err := writeArchive(&failingWriter{limit}, []sourceFile{{path: "run", body: "hello"}})
		if !errors.Is(err, errDestination) {
			t.Fatalf("writer limit %d: error = %v", limit, err)
		}
	}
}

func TestRenderMetadataAndOpaqueTemplate(t *testing.T) {
	source := testSource(sourceFile{"run", true, "print hello\n"})
	source.SetGeneration(7)
	source.SetLabels(map[string]string{"kapp.k14s.io/app": "deployment-owner"})
	source.SetAnnotations(map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "CR"})
	spec := source.Object["spec"].(map[string]interface{})
	spec["configMapMetadata"] = map[string]interface{}{
		"labels":      map[string]interface{}{"coalesce.flatheadmill.com/pipeline": ""},
		"annotations": map[string]interface{}{"coalesce.flatheadmill.com/repository": "example/hello"},
	}
	template := map[string]interface{}{
		"metadata": map[string]interface{}{"annotations": map[string]interface{}{"example/value": "on"}},
		"spec": map[string]interface{}{"futurePodField": map[string]interface{}{
			"nested": []interface{}{true, "on", "line\n", int64(9007199254740993)},
		}},
	}
	spec["template"] = template
	before := source.DeepCopy()
	result, err := renderConfigMap(source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatal("render mutated the source")
	}
	if result.Name != "hello" || result.Namespace != "coalesce" || len(result.Labels) != 1 || len(result.Annotations) != 2 ||
		result.Annotations[renderedGeneration] != "7" || result.Annotations["coalesce.flatheadmill.com/repository"] != "example/hello" {
		t.Fatalf("metadata = %#v", result.ObjectMeta)
	}
	owner := metav1.GetControllerOf(result)
	if owner == nil || owner.UID != source.GetUID() || owner.Name != source.GetName() ||
		owner.BlockOwnerDeletion == nil || *owner.BlockOwnerDeletion || len(result.OwnerReferences) != 1 {
		t.Fatalf("owner = %#v", owner)
	}
	gotJSON, err := yaml.YAMLToJSON([]byte(result.Data[templateKey]))
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(template)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("opaque template changed:\n%s\nwant:\n%s", gotJSON, wantJSON)
	}
	if got := unpack(t, result.BinaryData[archiveKey]); len(got) != 1 || got["run"].mode != 0755 {
		t.Fatalf("files = %#v", got)
	}
}

func TestTemplatePresence(t *testing.T) {
	for _, value := range []interface{}{nil, map[string]interface{}{}} {
		source := testSource()
		source.Object["spec"].(map[string]interface{})["template"] = value
		result, err := renderConfigMap(source)
		if err != nil {
			t.Fatal(err)
		}
		_, present := result.Data[templateKey]
		if present != (value != nil) {
			t.Fatalf("template %#v: data = %#v", value, result.Data)
		}
	}
	result, err := renderConfigMap(testSource())
	if err != nil || len(result.Data) != 0 || len(result.BinaryData) != 1 || len(unpack(t, result.BinaryData[archiveKey])) != 0 {
		t.Fatalf("empty library: result=%#v error=%v", result, err)
	}
}

func TestRenderRejectsReservedAnnotationAndOversize(t *testing.T) {
	source := testSource()
	spec := source.Object["spec"].(map[string]interface{})
	spec["configMapMetadata"] = map[string]interface{}{"annotations": map[string]interface{}{renderedGeneration: "1"}}
	if _, err := renderConfigMap(source); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("reserved annotation error = %v", err)
	}
	delete(spec, "configMapMetadata")
	spec["template"] = map[string]interface{}{"large": strings.Repeat("a", maxConfigMapBytes)}
	if _, err := renderConfigMap(source); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("size error = %v", err)
	}
}

func TestRenderRejectsMalformedFiles(t *testing.T) {
	for _, entry := range []interface{}{
		"not an object",
		map[string]interface{}{"body": "missing path"},
		map[string]interface{}{"path": "run"},
		map[string]interface{}{"path": "run", "body": "ok", "executable": "true"},
		map[string]interface{}{"path": "run", "body": "\xff"},
	} {
		source := testSource()
		source.Object["spec"].(map[string]interface{})["files"] = []interface{}{entry}
		if _, err := renderConfigMap(source); err == nil {
			t.Fatalf("accepted malformed entry %#v", entry)
		}
	}
	source := testSource()
	delete(source.Object["spec"].(map[string]interface{}), "files")
	if _, err := renderConfigMap(source); err == nil {
		t.Fatal("accepted missing files")
	}
}
