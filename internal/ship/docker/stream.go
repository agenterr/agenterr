package docker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/ship/process"
)

// Logs streams a container's stdout+stderr as process.Line, one per log
// line, from since onward. Each line's leading RFC3339Nano timestamp
// (requested via timestamps=1) is parsed into Line.Time and stripped from
// Line.Text; an unparsable prefix keeps the whole raw text with
// Line.Time = time.Now() and bumps UnparsedTimestamps (never dropped
// silently, per the ship semantics doc).
//
// Non-TTY containers get Docker's 8-byte-header multiplexed stream (stdout
// and stderr demuxed here into one line stream, in arrival order); TTY
// containers get a raw byte stream, detected via container inspect and read
// as-is. The returned channel closes on EOF or ctx cancellation.
func (c *Client) Logs(ctx context.Context, containerID string, since time.Time) (<-chan process.Line, error) {
	tty, err := c.inspectTTY(ctx, containerID)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/containers/%s/logs?follow=1&stdout=1&stderr=1&timestamps=1", containerID)
	if !since.IsZero() {
		path += fmt.Sprintf("&since=%d.%09d", since.Unix(), since.Nanosecond())
	}

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
		return nil, fmt.Errorf("docker: GET %s: %s", path, resp.Status)
	}

	out := make(chan process.Line)
	closeOnCancel(ctx, resp.Body)

	var src io.Reader = resp.Body
	if !tty {
		src = &demuxReader{r: resp.Body}
	}

	go func() {
		defer close(out)
		defer resp.Body.Close()

		br := bufio.NewReader(src)
		for {
			raw, err := br.ReadString('\n')
			if len(raw) > 0 {
				text := strings.TrimSuffix(raw, "\n")
				line := c.parseLine(text)
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				return // EOF, ctx-cancel body close, or a read error: stream is done
			}
		}
	}()

	return out, nil
}

// parseLine splits the RFC3339Nano timestamp prefix (the first
// space-delimited token) off text and parses it into a process.Line. A
// prefix that fails to parse as RFC3339Nano is not stripped: the whole raw
// text is kept, Time is set to time.Now(), and the client's unparsed-
// timestamp counter is incremented.
func (c *Client) parseLine(text string) process.Line {
	idx := strings.IndexByte(text, ' ')
	if idx < 0 {
		c.unparsedTimestamps.Add(1)
		return process.Line{Text: text, Time: time.Now()}
	}
	ts, rest := text[:idx], text[idx+1:]
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		c.unparsedTimestamps.Add(1)
		return process.Line{Text: text, Time: time.Now()}
	}
	return process.Line{Text: rest, Time: t}
}

// demuxReader strips Docker's 8-byte multiplex frame headers
// ([streamType, 0,0,0, bigEndianLen(4)]) from r, yielding the concatenated
// stdout+stderr payload bytes in arrival order. It uses io.ReadFull for
// both the header and payload reads, so a header or payload split across
// multiple underlying Read/chunk boundaries (e.g. the writer flushed
// mid-frame) is transparently reassembled — the caller never sees a torn
// frame.
type demuxReader struct {
	r         io.Reader
	remaining uint32 // payload bytes left in the frame currently being read
}

func (d *demuxReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for d.remaining == 0 {
		var hdr [8]byte
		if _, err := io.ReadFull(d.r, hdr[:]); err != nil {
			return 0, err // clean EOF between frames, or a torn stream at the very start
		}
		d.remaining = binary.BigEndian.Uint32(hdr[4:8])
		// A zero-length frame is legal (empty write) — loop to the next header.
	}

	n := len(p)
	if uint32(n) > d.remaining {
		n = int(d.remaining)
	}
	read, err := io.ReadFull(d.r, p[:n])
	d.remaining -= uint32(read)
	return read, err
}
