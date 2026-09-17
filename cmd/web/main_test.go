package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectTailPod(t *testing.T) {
	pod := func(name string, created int64, phase corev1.PodPhase) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(time.Unix(created, 0))},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	tests := []struct {
		name string
		pods []corev1.Pod
		want string
	}{
		{"empty", nil, ""},
		{"newest nonterminal before newer terminal", []corev1.Pod{
			pod("old-running", 10, corev1.PodRunning),
			pod("new-terminal", 40, corev1.PodFailed),
			pod("new-pending", 30, corev1.PodPending),
			pod("old-terminal", 20, corev1.PodSucceeded),
		}, "new-pending"},
		{"newest terminal fallback", []corev1.Pod{
			pod("old-failed", 10, corev1.PodFailed),
			pod("new-completed", 30, corev1.PodSucceeded),
			pod("middle-failed", 20, corev1.PodFailed),
		}, "new-completed"},
		{"equal timestamps use name", []corev1.Pod{
			pod("worker-b", 20, corev1.PodRunning),
			pod("worker-a", 20, corev1.PodRunning),
		}, "worker-a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Exercise every list permutation, including nonchronological ones.
			var check func(int)
			check = func(index int) {
				if index < len(test.pods) {
					for i := index; i < len(test.pods); i++ {
						test.pods[index], test.pods[i] = test.pods[i], test.pods[index]
						check(index + 1)
						test.pods[index], test.pods[i] = test.pods[i], test.pods[index]
					}
					return
				}
				got := selectTailPod(test.pods)
				if test.want == "" {
					if got != nil {
						t.Fatalf("got %s, want no Pod", got.Name)
					}
				} else if got == nil || got.Name != test.want {
					t.Fatalf("got %v, want %s", got, test.want)
				}
			}
			check(0)
		})
	}
}

type recordingEventWriter struct {
	lines []string
}

func (writer *recordingEventWriter) WriteJSON(value any) error {
	event, ok := value.(Event)
	if !ok {
		return fmt.Errorf("wrote %T, want Event", value)
	}
	data, ok := event.Data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("event data is %T", event.Data)
	}
	line, ok := data["line"].(string)
	if !ok {
		return fmt.Errorf("line is %T", data["line"])
	}
	writer.lines = append(writer.lines, line)
	return nil
}

func TestStreamLogLinesDoesNotDrop(t *testing.T) {
	const count = 5000
	var input strings.Builder
	for i := 0; i < count; i++ {
		fmt.Fprintf(&input, "line %d\n", i)
	}

	writer := new(recordingEventWriter)
	if err := streamLogLines(strings.NewReader(input.String()), writer, "build"); err != nil {
		t.Fatal(err)
	}
	if len(writer.lines) != count {
		t.Fatalf("wrote %d lines, want %d", len(writer.lines), count)
	}
	for i, line := range writer.lines {
		want := fmt.Sprintf("line %d", i)
		if line != want {
			t.Fatalf("line %d = %q, want %q", i, line, want)
		}
	}
}

func TestStreamLogLinesAcceptsLargeLine(t *testing.T) {
	line := strings.Repeat("x", 128*1024)
	writer := new(recordingEventWriter)
	if err := streamLogLines(strings.NewReader(line+"\n"), writer, "build"); err != nil {
		t.Fatal(err)
	}
	if len(writer.lines) != 1 || writer.lines[0] != line {
		t.Fatalf("large line was not preserved")
	}
}
