// Command agenterr-mcp is a dumb stdio→HTTP proxy: it speaks MCP over
// stdio to a local agent (the SDK's stdio/IO server transport) and
// forwards every tools/list and tools/call request verbatim to a remote
// Agenterr server's Streamable HTTP /mcp endpoint, injecting the API key
// as a bearer token. It carries no knowledge of Agenterr's tools — new
// tools added to internal/mcp show up here with zero proxy changes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const usage = "usage: agenterr-mcp --url https://host --key agt_api_... (or AGENTERR_URL / AGENTERR_API_KEY)"

func main() {
	cfg, err := resolveConfig(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}

	if err := run(context.Background(), cfg.url, cfg.key, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "agenterr-mcp:", err)
		os.Exit(1)
	}
}

// config is the proxy's resolved (flag-or-env) configuration.
type config struct {
	url string
	key string
}

// errMissingConfig is returned by resolveConfig when --url/AGENTERR_URL or
// --key/AGENTERR_API_KEY are unset after resolution; main prints usage and
// exits 2 on it.
var errMissingConfig = errors.New("missing --url/--key (or AGENTERR_URL/AGENTERR_API_KEY)")

// resolveConfig resolves the proxy's URL and key from flags first, env
// vars second — flags win. Missing either after resolution is an error.
// The URL is normalized to end in /mcp (appended if absent) so callers may
// pass either the bare server origin or the full endpoint.
func resolveConfig(args []string, getenv func(string) string) (config, error) {
	fs := flag.NewFlagSet("agenterr-mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	urlFlag := fs.String("url", "", "Agenterr server URL, e.g. https://logs.example.com")
	keyFlag := fs.String("key", "", "Agenterr API key (agt_api_... or agt_admin_...)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	u := *urlFlag
	if u == "" {
		u = getenv("AGENTERR_URL")
	}
	k := *keyFlag
	if k == "" {
		k = getenv("AGENTERR_API_KEY")
	}
	if u == "" || k == "" {
		return config{}, errMissingConfig
	}

	return config{url: normalizeMCPURL(u), key: k}, nil
}

// normalizeMCPURL appends the /mcp path if u doesn't already end in it.
func normalizeMCPURL(u string) string {
	u = strings.TrimSuffix(u, "/")
	if strings.HasSuffix(u, "/mcp") {
		return u
	}
	return u + "/mcp"
}

// run is the proxy's core: it connects to the remote Streamable HTTP
// endpoint at url (failing fast on error), then serves MCP over stdin/
// stdout, forwarding every tools/list and tools/call request to the
// remote session verbatim. It returns when stdin closes (client
// disconnect) or ctx is cancelled; a nil return means clean shutdown.
func run(ctx context.Context, url, key string, stdin io.ReadCloser, stdout io.WriteCloser) error {
	remoteClient := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "agenterr-mcp-proxy", Version: "0.1.0"}, nil)
	transport := &mcpsdk.StreamableClientTransport{
		Endpoint:   url,
		HTTPClient: &http.Client{Transport: bearerTransport{key: key}},
	}
	remote, err := remoteClient.Connect(ctx, transport, nil)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", url, err)
	}
	defer remote.Close()

	local := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "agenterr-mcp-proxy", Version: "0.1.0"}, &mcpsdk.ServerOptions{
		HasTools: true, // no local tools registered — every one lives remotely
	})
	local.AddReceivingMiddleware(proxyMiddleware(remote))

	return local.Run(ctx, &mcpsdk.IOTransport{Reader: stdin, Writer: stdout})
}

// proxyMiddleware forwards tools/list and tools/call requests verbatim to
// remote, and lets everything else fall through to the local server's
// default handling. This is the entire proxy: no tool knowledge, no
// caching — the remote server's tool set is whatever the client sees.
func proxyMiddleware(remote *mcpsdk.ClientSession) mcpsdk.Middleware {
	return func(next mcpsdk.MethodHandler) mcpsdk.MethodHandler {
		return func(ctx context.Context, method string, req mcpsdk.Request) (mcpsdk.Result, error) {
			switch method {
			case "tools/list":
				// Rebuild bare params (just the pagination cursor) rather
				// than forwarding req.GetParams() verbatim: the incoming
				// params carry the local session's request Meta (protocol
				// version, client info), which is meaningless — and
				// rejected — on the unrelated remote session.
				var cursor string
				if p, ok := req.GetParams().(*mcpsdk.ListToolsParams); ok && p != nil {
					cursor = p.Cursor
				}
				return remote.ListTools(ctx, &mcpsdk.ListToolsParams{Cursor: cursor})
			case "tools/call":
				raw, ok := req.GetParams().(*mcpsdk.CallToolParamsRaw)
				if !ok {
					return next(ctx, method, req)
				}
				return remote.CallTool(ctx, &mcpsdk.CallToolParams{
					Name:      raw.Name,
					Arguments: raw.Arguments,
				})
			default:
				return next(ctx, method, req)
			}
		}
	}
}

// bearerTransport injects "Authorization: Bearer <key>" on every outbound
// request to the remote server, mirroring the pattern in
// internal/mcp/tools_test.go's over-the-wire test.
type bearerTransport struct {
	key string
}

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.key)
	return http.DefaultTransport.RoundTrip(req)
}
