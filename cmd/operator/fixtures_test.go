//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// This is deliberately the real make command and the real extraction tool.
// It compares the consumer's tree, metadata and template, not two tar formats'
// compressed byte streams. Run with go test -tags=integration ./cmd/operator.
func TestDirectoryAndResourceFixturesAreConsumerEquivalent(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hello", "library"} {
		t.Run(name, func(t *testing.T) {
			fixture, err := os.ReadFile(filepath.Join(root, "test", name+".coalesce.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := yaml.YAMLToJSON(fixture)
			if err != nil {
				t.Fatal(err)
			}
			var source unstructured.Unstructured
			if err := source.UnmarshalJSON(encoded); err != nil {
				t.Fatal(err)
			}
			source.SetUID("fixture-uid")
			source.SetGeneration(1)
			generated, err := renderConfigMap(&source)
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(filepath.Join(root, "bin/coalesce"), "make", "test/"+name)
			command.Dir = root
			command.Env = append(os.Environ(), "TMPDIR="+t.TempDir())
			var stderr bytes.Buffer
			command.Stderr = &stderr
			made, err := command.Output()
			if err != nil {
				t.Fatalf("make: %v\n%s", err, &stderr)
			}
			var direct corev1.ConfigMap
			if err := yaml.UnmarshalStrict(made, &direct); err != nil {
				t.Fatal(err)
			}
			delete(generated.Annotations, renderedGeneration)
			if len(generated.Annotations) == 0 {
				generated.Annotations = nil
			}
			if generated.Name != direct.Name || generated.Namespace != direct.Namespace ||
				!reflect.DeepEqual(generated.Labels, direct.Labels) || !reflect.DeepEqual(generated.Annotations, direct.Annotations) {
				t.Fatalf("consumer metadata differs: generated=%#v direct=%#v", generated.ObjectMeta, direct.ObjectMeta)
			}
			if !reflect.DeepEqual(templateValue(t, generated.Data[templateKey]), templateValue(t, direct.Data[templateKey])) {
				t.Fatal("template changed between producers")
			}
			if len(generated.BinaryData) != 1 || len(direct.BinaryData) != 1 {
				t.Fatal("unexpected binary keys")
			}
			want := unpack(t, direct.BinaryData[archiveKey])
			if !reflect.DeepEqual(unpack(t, generated.BinaryData[archiveKey]), want) {
				t.Fatal("archive trees or modes differ")
			}
			destination := t.TempDir()
			extract := exec.Command("bsdtar", "-xzf", "-", "-C", destination)
			extract.Stdin = bytes.NewReader(generated.BinaryData[archiveKey])
			if output, err := extract.CombinedOutput(); err != nil {
				t.Fatalf("extract: %v\n%s", err, output)
			}
			for path, expected := range want {
				body, err := os.ReadFile(filepath.Join(destination, path))
				if err != nil || string(body) != expected.body {
					t.Fatalf("extracted %s: %v, content mismatch=%v", path, err, string(body) != expected.body)
				}
				info, err := os.Stat(filepath.Join(destination, path))
				if err != nil || int64(info.Mode().Perm()) != expected.mode {
					t.Fatalf("extracted mode of %s: %v", path, err)
				}
			}
		})
	}
}

func templateValue(t *testing.T, text string) interface{} {
	t.Helper()
	if text == "" {
		return nil
	}
	encoded, err := yaml.YAMLToJSON([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	var value interface{}
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
