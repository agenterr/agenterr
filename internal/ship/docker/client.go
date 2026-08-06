// Package docker is a minimal Docker Engine API client over a unix socket,
// stdlib only. It covers exactly what agenterr-ship needs: listing running
// containers, a lifecycle event stream, and demuxed log tailing — plus the
// service-naming and container-selection rules from the ship semantics doc.
package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
)

// Container is a running container as returned by /containers/json.
type Container struct {
	ID     string
	Name   string // leading "/" already stripped
	Labels map[string]string
}

// Event is a container lifecycle event from /events (start/die).
type Event struct {
	Action string
	ID     string
}

// Client talks to the Docker Engine API over a unix socket.
type Client struct {
	httpc    *http.Client
	sockPath string

	// unparsedTimestamps counts log lines whose leading token wasn't a
	// valid RFC3339Nano timestamp (see stream.go's parseLine). Never lost
	// silently: exposed via UnparsedTimestamps for the caller's periodic
	// self-log line.
	unparsedTimestamps atomic.Int64
}

// NewClient returns a Client dialing the Docker Engine API over the unix
// socket at sockPath (typically /var/run/docker.sock).
func NewClient(sockPath string) *Client {
	return &Client{
		sockPath: sockPath,
		httpc: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sockPath)
				},
			},
		},
	}
}

// UnparsedTimestamps returns the count of log lines with an unparsable
// timestamp prefix seen so far (see stream.go).
func (c *Client) UnparsedTimestamps() int64 {
	return c.unparsedTimestamps.Load()
}

// url builds a request URL for the given path; the host is ignored by the
// unix-socket transport.
func (c *Client) url(path string) string {
	return "http://docker" + path
}

// get issues a GET request against the socket and returns the response,
// erroring out (and closing the body) on any non-2xx status.
func (c *Client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("docker: GET %s: %s: %s", path, resp.Status, string(body))
	}
	return resp, nil
}

// Ping checks that the Docker daemon is reachable and responding.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.get(ctx, "/_ping")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// rawContainer mirrors the subset of /containers/json's element shape we use.
type rawContainer struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
}

// Containers returns the currently running containers (Docker's default
// /containers/json filter — no all=1).
func (c *Client) Containers(ctx context.Context) ([]Container, error) {
	resp, err := c.get(ctx, "/containers/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var raws []rawContainer
	if err := json.NewDecoder(resp.Body).Decode(&raws); err != nil {
		return nil, fmt.Errorf("docker: decode /containers/json: %w", err)
	}

	out := make([]Container, 0, len(raws))
	for _, r := range raws {
		name := r.ID
		if len(r.Names) > 0 {
			name = strings.TrimPrefix(r.Names[0], "/")
		}
		out = append(out, Container{ID: r.ID, Name: name, Labels: r.Labels})
	}
	return out, nil
}

// rawInspect mirrors the subset of /containers/{id}/json we use.
type rawInspect struct {
	Config struct {
		Tty bool `json:"Tty"`
	} `json:"Config"`
}

// inspectTTY reports whether the container was started with a TTY attached
// (Config.Tty) — Docker sends raw, non-multiplexed log output for such
// containers instead of the 8-byte-header framed stream.
func (c *Client) inspectTTY(ctx context.Context, containerID string) (bool, error) {
	resp, err := c.get(ctx, "/containers/"+containerID+"/json")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var ri rawInspect
	if err := json.NewDecoder(resp.Body).Decode(&ri); err != nil {
		return false, fmt.Errorf("docker: decode /containers/%s/json: %w", containerID, err)
	}
	return ri.Config.Tty, nil
}

// rawEvent mirrors the subset of a /events JSONL record we use.
type rawEvent struct {
	Action string `json:"Action"`
	Actor  struct {
		ID string `json:"ID"`
	} `json:"Actor"`
}

// eventsFilters is the fixed filter Docker expects as a JSON-encoded query
// param: container start/die only.
const eventsFilters = `{"type":["container"],"event":["start","die"]}`

// Events streams container start/die lifecycle events. The returned channel
// closes when ctx is cancelled or the underlying connection ends; any
// stream error after at least one successful connect is treated as EOF (the
// caller reconnects per the ship semantics doc's reconnect-with-backoff
// rule — this Client does not retry internally).
func (c *Client) Events(ctx context.Context) (<-chan Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.url("/events?filters="+eventsFilters), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("docker: GET /events: %s", resp.Status)
	}

	out := make(chan Event)
	closeOnCancel(ctx, resp.Body)

	go func() {
		defer close(out)
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)
		for {
			var re rawEvent
			if err := dec.Decode(&re); err != nil {
				return // EOF, ctx-cancel body close, or decode error: stream is done
			}
			select {
			case out <- Event{Action: re.Action, ID: re.Actor.ID}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// closeOnCancel closes body as soon as ctx is done, unblocking whatever
// goroutine is currently reading it. It leaks nothing: the watcher
// goroutine itself exits once the read loop finishes, via the returned
// stop func's channel — callers close `done` when their loop returns.
func closeOnCancel(ctx context.Context, body io.Closer) {
	go func() {
		<-ctx.Done()
		body.Close()
	}()
}

// ServiceName derives the service name for a container per the ship
// semantics label chain: swarm service label, then compose service label,
// then the container's own name — sanitized to [a-zA-Z0-9_-], every other
// rune replaced with '_'.
func ServiceName(ct Container) string {
	name := ct.Labels["com.docker.swarm.service.name"]
	if name == "" {
		name = ct.Labels["com.docker.compose.service"]
	}
	if name == "" {
		name = ct.Name
	}
	return sanitizeServiceName(name)
}

func sanitizeServiceName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Selected reports whether a container should be tailed, given its derived
// service name and labels against the --only/--exclude lists:
//   - agenterr.ignore=true always excludes.
//   - --only set: selected iff name is in only AND not in exclude
//     ("only-list minus exclude-list").
//   - --only empty: selected iff name is not in exclude.
func Selected(name string, labels map[string]string, only, exclude []string) bool {
	if labels["agenterr.ignore"] == "true" {
		return false
	}
	if len(only) > 0 {
		return contains(only, name) && !contains(exclude, name)
	}
	return !contains(exclude, name)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
