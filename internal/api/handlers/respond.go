// Package handlers implements the /api/v1 route handlers, split by domain
// (projects, issues, logs, stats). Each handler decodes its request, calls
// into store.Reader/store.Admin, and encodes the result via respond.
package handlers

import (
	"encoding/json"
	"net/http"
)

// respond writes v as a JSON response body with the given status code. A
// nil v writes the status code with no body (used for 204 responses).
func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// respondErr writes a uniform {"error": msg} JSON body with the given
// status code.
func respondErr(w http.ResponseWriter, code int, msg string) {
	respond(w, code, map[string]string{"error": msg})
}
