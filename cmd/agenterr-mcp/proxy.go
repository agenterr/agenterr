package main

import (
	"context"
	"net/http"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// proxyMiddleware forwards tools/list and tools/call requests to remote,
// and lets everything else fall through to the local server's default
// handling. This is the entire proxy: no tool knowledge, no caching — the
// remote server's tool set is whatever the client sees. Every field of the
// incoming params is forwarded except the local session's stamped Meta
// keys — see stripLocalSessionMeta — so elicitation retries, progress
// tokens, and pagination all survive the hop.
func proxyMiddleware(remote *mcpsdk.ClientSession) mcpsdk.Middleware {
	return func(next mcpsdk.MethodHandler) mcpsdk.MethodHandler {
		return func(ctx context.Context, method string, req mcpsdk.Request) (mcpsdk.Result, error) {
			switch method {
			case "tools/list":
				// ListToolsParams has exactly two fields: Meta and
				// Cursor. Both forwarded (Meta stripped of local-session
				// keys, see stripLocalSessionMeta) — re-check this list
				// if the SDK adds fields to ListToolsParams.
				var params *mcpsdk.ListToolsParams
				if p, ok := req.GetParams().(*mcpsdk.ListToolsParams); ok && p != nil {
					params = &mcpsdk.ListToolsParams{
						Meta:   stripLocalSessionMeta(p.Meta),
						Cursor: p.Cursor,
					}
				}
				return remote.ListTools(ctx, params)
			case "tools/call":
				raw, ok := req.GetParams().(*mcpsdk.CallToolParamsRaw)
				if !ok {
					return next(ctx, method, req)
				}
				// CallToolParamsRaw has five fields: Meta, Name,
				// Arguments, InputResponses, RequestState. All five are
				// forwarded below (Meta stripped, see
				// stripLocalSessionMeta) — re-check this list if the SDK
				// adds fields to CallToolParamsRaw/CallToolParams on
				// upgrade, since a naive struct-literal copy silently
				// drops anything not named here (this is exactly the bug
				// being fixed: an earlier version forwarded only Name
				// and Arguments, silently dropping InputResponses and
				// RequestState, breaking elicitation retries).
				return remote.CallTool(ctx, &mcpsdk.CallToolParams{
					Meta:           stripLocalSessionMeta(raw.Meta),
					Name:           raw.Name,
					Arguments:      raw.Arguments,
					InputResponses: raw.InputResponses,
					RequestState:   raw.RequestState,
				})
			default:
				return next(ctx, method, req)
			}
		}
	}
}

// stripLocalSessionMeta copies m, deleting the three keys the go-sdk
// client stamps into every outgoing request's Meta for its *own* session
// (mcpsdk.MetaKeyProtocolVersion, MetaKeyClientInfo,
// MetaKeyClientCapabilities — see (*ClientSession).injectRequestMeta in
// the SDK). Forwarding those verbatim from the local session to the
// remote session breaks it: the remote server sees a protocol
// version/capabilities pair it never negotiated for that session and
// rejects the request (this is the bug the original version of this
// proxy hit — 400 "protocol version ... is only supported on stateless
// HTTP servers"). Once stripped, remote.ListTools/CallTool re-stamp the
// same three keys with values matching the *remote* session, via the
// same injectRequestMeta mechanism, because the keys are then absent.
// Every other Meta entry — notably a caller's progress token — is a
// legitimate part of the request and passes through untouched.
func stripLocalSessionMeta(m mcpsdk.Meta) mcpsdk.Meta {
	if len(m) == 0 {
		return m
	}
	out := make(mcpsdk.Meta, len(m))
	for k, v := range m {
		out[k] = v
	}
	delete(out, mcpsdk.MetaKeyProtocolVersion)
	delete(out, mcpsdk.MetaKeyClientInfo)
	delete(out, mcpsdk.MetaKeyClientCapabilities)
	return out
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
