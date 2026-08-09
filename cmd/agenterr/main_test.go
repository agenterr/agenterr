package main

import (
	"reflect"
	"testing"
)

func TestDispatchTarget(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCmd  string
		wantRest []string
	}{
		{
			name:     "bare invocation serves, unchanged",
			args:     nil,
			wantCmd:  "",
			wantRest: nil,
		},
		{
			name:     "--version still serves (main handles it on the serve path)",
			args:     []string{"--version"},
			wantCmd:  "",
			wantRest: []string{"--version"},
		},
		{
			name:     "serve flags untouched",
			args:     []string{"--listen", ":9000", "--db", "/tmp/x.db"},
			wantCmd:  "",
			wantRest: []string{"--listen", ":9000", "--db", "/tmp/x.db"},
		},
		{
			name:     "ship subcommand dispatches with remaining args",
			args:     []string{"ship", "--url", "https://x", "--key", "k", "--docker"},
			wantCmd:  "ship",
			wantRest: []string{"--url", "https://x", "--key", "k", "--docker"},
		},
		{
			name:     "ship with no further args",
			args:     []string{"ship"},
			wantCmd:  "ship",
			wantRest: []string{},
		},
		{
			name: "ship only matches as the first argument",
			// A flag happening to carry the literal value "ship" downstream
			// must not be mistaken for the subcommand.
			args:     []string{"--db", "ship"},
			wantCmd:  "",
			wantRest: []string{"--db", "ship"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, rest := dispatchTarget(tt.args)
			if cmd != tt.wantCmd {
				t.Errorf("cmd = %q, want %q", cmd, tt.wantCmd)
			}
			if !reflect.DeepEqual(rest, tt.wantRest) && (len(rest) != 0 || len(tt.wantRest) != 0) {
				t.Errorf("rest = %v, want %v", rest, tt.wantRest)
			}
		})
	}
}
