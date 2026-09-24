package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

var coalesceResource = schema.GroupVersionResource{
	Group: "coalesce.flatheadmill.com", Version: "v1alpha1", Resource: "coalesces",
}

const (
	coalesceKind       = "Coalesce"
	archiveKey         = "ball.tar.gz"
	templateKey        = "template.yaml"
	renderedGeneration = "coalesce.flatheadmill.com/rendered-generation"
	maxConfigMapBytes  = 1 << 20
)

type sourceFile struct {
	path       string
	executable bool
	body       string
}

// The template stays unstructured all the way to its serialized form. The
// operator's Kubernetes client version must not become another Pod schema.
func renderConfigMap(source *unstructured.Unstructured) (*corev1.ConfigMap, error) {
	labels, _, err := unstructured.NestedStringMap(source.Object, "spec", "configMapMetadata", "labels")
	if err != nil {
		return nil, err
	}
	if len(labels) == 0 {
		labels = nil
	}
	annotations, _, err := unstructured.NestedStringMap(source.Object, "spec", "configMapMetadata", "annotations")
	if err != nil {
		return nil, err
	}
	if _, reserved := annotations[renderedGeneration]; reserved {
		return nil, fmt.Errorf("spec.configMapMetadata.annotations[%q] is reserved for the operator", renderedGeneration)
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[renderedGeneration] = strconv.FormatInt(source.GetGeneration(), 10)

	files, err := filesFromResource(source)
	if err != nil {
		return nil, err
	}
	var archive bytes.Buffer
	if err := writeArchive(&archive, files); err != nil {
		return nil, err
	}

	controller, blockDeletion := true, false
	result := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: source.GetName(), Namespace: source.GetNamespace(),
			Labels: labels, Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: coalesceResource.GroupVersion().String(), Kind: coalesceKind,
				Name: source.GetName(), UID: source.GetUID(),
				Controller: &controller, BlockOwnerDeletion: &blockDeletion,
			}},
		},
		BinaryData: map[string][]byte{archiveKey: archive.Bytes()},
	}
	template, present, err := unstructured.NestedFieldNoCopy(source.Object, "spec", "template")
	if err != nil {
		return nil, err
	}
	if present && template != nil {
		if _, object := template.(map[string]interface{}); !object {
			return nil, fmt.Errorf("spec.template must be an object or null")
		}
		encoded, err := json.Marshal(template)
		if err != nil {
			return nil, fmt.Errorf("encode spec.template: %w", err)
		}
		encoded, err = yaml.JSONToYAML(encoded)
		if err != nil {
			return nil, fmt.Errorf("encode spec.template as YAML: %w", err)
		}
		result.Data = map[string]string{templateKey: string(encoded)}
	}
	if len(result.BinaryData[archiveKey])+len(result.Data[templateKey]) > maxConfigMapBytes {
		return nil, fmt.Errorf("rendered ConfigMap data exceeds the Kubernetes limit of %d bytes", maxConfigMapBytes)
	}
	return result, nil
}

func filesFromResource(source *unstructured.Unstructured) ([]sourceFile, error) {
	values, present, err := unstructured.NestedSlice(source.Object, "spec", "files")
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, fmt.Errorf("spec.files is required")
	}
	files := make([]sourceFile, 0, len(values))
	for i, value := range values {
		object, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("spec.files[%d] must be an object", i)
		}
		name, hasPath, err := unstructured.NestedString(object, "path")
		if err != nil || !hasPath {
			return nil, fmt.Errorf("spec.files[%d].path must be a string", i)
		}
		body, hasBody, err := unstructured.NestedString(object, "body")
		if err != nil || !hasBody || !utf8.ValidString(body) {
			return nil, fmt.Errorf("spec.files[%d].body must be a UTF-8 string", i)
		}
		executable, _, err := unstructured.NestedBool(object, "executable")
		if err != nil {
			return nil, fmt.Errorf("spec.files[%d].executable must be a boolean", i)
		}
		files = append(files, sourceFile{path: name, executable: executable, body: body})
	}
	return files, nil
}

func writeArchive(destination io.Writer, files []sourceFile) error {
	// Validate the whole tree before emitting it. Two individually clean paths
	// can still ask tar to extract a file where it needs a directory.
	names := make(map[string]bool, len(files))
	for _, file := range files {
		name := file.path
		if name == "" || name == "." || name == ".." || path.IsAbs(name) ||
			path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.ContainsRune(name, 0) {
			return fmt.Errorf("file path %q must be a clean relative path", name)
		}
		if names[name] {
			return fmt.Errorf("duplicate file path %q", name)
		}
		names[name] = true
	}
	for name := range names {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if names[parent] {
				return fmt.Errorf("file path %q is an ancestor of %q", parent, name)
			}
		}
	}
	ordered := append([]sourceFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].path < ordered[j].path })
	compressed, err := gzip.NewWriterLevel(destination, gzip.BestCompression)
	if err != nil {
		return err
	}
	compressed.Header = gzip.Header{OS: 255}
	archive := tar.NewWriter(compressed)
	for _, file := range ordered {
		mode := int64(0644)
		if file.executable {
			mode = 0755
		}
		header := &tar.Header{
			Name: file.path, Typeflag: tar.TypeReg, Size: int64(len(file.body)), Mode: mode,
			Uid: 0, Gid: 0, Uname: "root", Gname: "root", ModTime: time.Unix(0, 0),
			Format: tar.FormatUSTAR,
		}
		if err := archive.WriteHeader(header); err != nil {
			return fmt.Errorf("write USTAR header for %q: %w", file.path, err)
		}
		if _, err := io.WriteString(archive, file.body); err != nil {
			return fmt.Errorf("write file %q: %w", file.path, err)
		}
	}
	if err := archive.Close(); err != nil {
		return fmt.Errorf("close tar: %w", err)
	}
	if err := compressed.Close(); err != nil {
		return fmt.Errorf("close gzip: %w", err)
	}
	return nil
}
