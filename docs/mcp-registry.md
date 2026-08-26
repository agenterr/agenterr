# Publishing to the official MCP Registry

`server.json` in the repo root describes agenterr for the
[official MCP Registry](https://registry.modelcontextprotocol.io). Most MCP
directories ingest from it — PulseMCP, for instance, pauses its own
submissions and points publishers here — so this is the single listing that
feeds the rest.

## The shape, and why

Agenterr does not fit the registry's common cases. It is neither an npm/PyPI
package that a client launches over stdio, nor a public remote server at a
fixed URL. It is software you host, whose MCP endpoint lives on your own
instance.

The honest encoding is an **OCI package with a streamable-http transport**:
run `ghcr.io/agenterr/agenterr`, and it serves MCP at
`http://localhost:3617/mcp` behind an `Authorization: Bearer` key. That is
what `server.json` declares. A `remotes` entry would be wrong — the registry
requires those to be publicly reachable, and there is no hosted agenterr.

## Ownership verification

The registry proves ownership by reading the
`io.modelcontextprotocol.server.name` label off the published image and
checking it equals `name` in `server.json`. That label is set in the
`Dockerfile`.

**This creates an ordering constraint.** The label only exists on images built
after it was added, so publishing must follow a release that carries it:

1. Tag and release a version whose image has the label (v0.2.1 or later).
2. Confirm the label is on the pushed image:
   ```
   docker buildx imagetools inspect ghcr.io/agenterr/agenterr:<version> --format '{{ json .Provenance }}'
   ```
   or pull it and run `docker inspect`.
3. Make sure `version` and the `packages[0].identifier` tag in `server.json`
   both match that release.
4. Publish:
   ```
   mcp-publisher login github
   mcp-publisher validate
   mcp-publisher publish
   ```

Publishing before such a release exists fails verification, because the
registry looks for a label the published image does not carry yet.

## Naming

The server name is `io.github.agenterr/agenterr`. GitHub-based authentication
requires the `io.github.<owner>/` prefix, and the owner has to be the
`agenterr` org that holds the repo — so this name is only publishable by
someone with access to it.
