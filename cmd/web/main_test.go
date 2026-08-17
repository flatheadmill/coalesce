package main

import (
	"fmt"
	"strings"
	"testing"
)

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
