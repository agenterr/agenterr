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
	neturl "net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const usage = "usage: agenterr-mcp --url https://host --key agt_api_... (or AGENTERR_URL / AGENTERR_API_KEY)"

func main() {
	cfg, err := resolveConfig(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}

	// SIGINT/SIGTERM cancel ctx rather than killing the process outright,
	// so run() gets a chance to close the remote session cleanly instead
	// of leaving it to time out server-side.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg.url, cfg.key, os.Stdin, os.Stdout); err != nil {
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
// disconnect) or ctx is cancelled; a nil return means clean shutdown —
// including shutdown triggered by ctx cancellation (e.g. SIGINT/SIGTERM
// via signal.NotifyContext in main), which is not itself an error.
func run(ctx context.Context, url, key string, stdin io.ReadCloser, stdout io.WriteCloser) error {
	remoteClient := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "agenterr-mcp-proxy", Version: "0.1.0"}, nil)
	transport := &mcpsdk.StreamableClientTransport{
		Endpoint:   url,
		HTTPClient: &http.Client{Transport: bearerTransport{key: key}},
	}
	remote, err := remoteClient.Connect(ctx, transport, nil)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", redactURL(url), err)
	}
	// Runs on every return path below, so ctx cancellation, a local.Run
	// error, and clean client disconnect all end with the remote session
	// explicitly closed rather than abandoned to an idle timeout.
	defer remote.Close()

	local := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "agenterr-mcp-proxy", Version: "0.1.0"}, &mcpsdk.ServerOptions{
		HasTools: true, // no local tools registered — every one lives remotely
	})
	local.AddReceivingMiddleware(proxyMiddleware(remote))

	done := make(chan error, 1)
	go func() {
		done <- local.Run(ctx, &mcpsdk.IOTransport{Reader: stdin, Writer: stdout})
	}()

	select {
	case <-ctx.Done():
		// local.Run also observes ctx (mcp.Server.Run selects on it
		// internally) and is already tearing its own session down; wait
		// for that to finish so the deferred remote.Close() above runs
		// only once local resources are released, then report a clean
		// shutdown rather than surfacing ctx.Err() as a proxy failure.
		<-done
		return nil
	case err := <-done:
		return err
	}
}

// redactURL strips embedded userinfo (user:password@) from raw before it
// can land in an error message that gets logged or shown to an agent. If
// raw doesn't parse as a URL, it's returned unchanged — the connect error
// it's embedded in is informative either way.
func redactURL(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Redacted()
}
