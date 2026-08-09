package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// deliveryAttempts is the total number of POST attempts per fire (1
// initial + 2 retries), per the alert semantics doc's 1s/4s backoff.
const deliveryAttempts = 3

// recordTimeout bounds RecordAlertResult calls made after delivery,
// including during Run's shutdown drain — that call must not be tied to
// the (already-canceled) run context, or a drained delivery's outcome
// would never make it to the store.
const recordTimeout = 5 * time.Second

// fireJob carries everything the delivery worker needs to build and send
// one webhook payload, captured at enqueue time on the writer goroutine
// (so delivery itself never touches store.Entry/IssueOutcome, keeping the
// worker store-free beyond RecordAlertResult).
type fireJob struct {
	rule          store.AlertRuleRow
	issueID       int64
	title         string
	severity      core.Severity
	eventTime     time.Time
	eventCount    int // threshold rules only
	windowMinutes int // threshold rules only
	test          bool
}

// alertPayload is the webhook body shape from the alert semantics doc.
// issue.count is a deliberate deviation from a literal issue-row read:
// the delivery worker is store-free on the hot path, so for threshold
// rules count is the window's event_count (the number that triggered the
// rule); for new_issue/regression it's omitted (0) since a live count
// would require a DB read this path intentionally avoids.
type alertPayload struct {
	Rule struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Kind string `json:"kind"`
	} `json:"rule"`
	ProjectID int64 `json:"project_id"`
	Issue     struct {
		ID        int64  `json:"id"`
		Title     string `json:"title"`
		Severity  string `json:"severity"`
		Count     int64  `json:"count"`
		FirstSeen string `json:"first_seen"`
		LastSeen  string `json:"last_seen"`
	} `json:"issue"`
	EventCount    int    `json:"event_count,omitempty"`
	WindowMinutes int    `json:"window_minutes,omitempty"`
	FiredAt       string `json:"fired_at"`
	Test          bool   `json:"test,omitempty"`
}

// buildPayload renders job into the wire shape. firstSeen/lastSeen both
// use the triggering event's log time — IssueEvent only has that
// timestamp available (not the issue's true first-seen), which is the
// same store-free trade-off as issue.count.
func buildPayload(job fireJob) alertPayload {
	var p alertPayload
	p.Rule.ID = job.rule.ID
	p.Rule.Name = job.rule.Name
	p.Rule.Kind = string(job.rule.Kind)
	p.ProjectID = job.rule.ProjectID
	p.Issue.ID = job.issueID
	p.Issue.Title = job.title
	p.Issue.Severity = job.severity.String()
	p.Issue.FirstSeen = job.eventTime.UTC().Format(time.RFC3339)
	p.Issue.LastSeen = job.eventTime.UTC().Format(time.RFC3339)
	if job.rule.Kind == core.AlertThreshold {
		p.Issue.Count = int64(job.eventCount)
		p.EventCount = job.eventCount
		p.WindowMinutes = job.windowMinutes
	}
	p.FiredAt = time.Now().UTC().Format(time.RFC3339)
	p.Test = job.test
	return p
}

// Run consumes the delivery queue until ctx is canceled, then drains
// whatever is left in the queue (attempting delivery for each) before
// returning — a canceled context stops accepting new work but never
// abandons work already enqueued.
//
// Precondition: callers must stop invoking IssueEvent before canceling
// ctx. The drain loop below is a single best-effort pass over the queue;
// an enqueue that races with (or follows) cancellation may be neither
// delivered nor counted as a drop. In production this holds because the
// app stops the pipeline — the only IssueEvent caller — before canceling
// the alerts worker.
func (e *Engine) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case job := <-e.queue:
					e.deliver(job)
				default:
					return
				}
			}
		case job := <-e.queue:
			e.deliver(job)
		}
	}
}

// deliver sends job's payload with retries and records the outcome.
func (e *Engine) deliver(job fireJob) {
	payload := buildPayload(job)
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("alerts: failed to marshal payload", "rule_id", job.rule.ID, "error", err)
		return
	}

	lastErr := e.sendWithRetry(job.rule, body)
	e.recordResult(job.rule.ID, lastErr)
}

// sendWithRetry attempts delivery up to deliveryAttempts times, sleeping
// e.backoff[i] between attempt i+1 and i+2. It returns the last attempt's
// error, or nil once any attempt succeeds. The background worker has no
// caller-supplied context to thread through, unlike TestFire.
func (e *Engine) sendWithRetry(rule store.AlertRuleRow, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < deliveryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(e.backoff[attempt-1])
		}
		lastErr = e.attemptDelivery(context.Background(), rule, body)
		if lastErr == nil {
			return nil
		}
	}
	return lastErr
}

// attemptDelivery makes a single POST to rule's webhook. Success is any
// 2xx status.
func (e *Engine) attemptDelivery(ctx context.Context, rule store.AlertRuleRow, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rule.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range rule.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

// recordResult persists a delivery outcome using a fresh background-ish
// context (not the worker's run ctx, which may already be canceled during
// shutdown drain) so every attempt — including drained ones — lands in
// last_fired/last_error per the no-silent-failure rule.
func (e *Engine) recordResult(ruleID int64, deliveryErr error) {
	errMsg := ""
	if deliveryErr != nil {
		errMsg = deliveryErr.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()
	if err := e.ar.RecordAlertResult(ctx, ruleID, time.Now(), errMsg); err != nil {
		slog.Error("alerts: failed to record delivery result", "rule_id", ruleID, "error", err)
	}
}

// TestFire delivers rule id's webhook synchronously with a sample issue
// and "test":true, recording the outcome the same way a real fire would.
// It returns the delivery error (nil on 2xx), so callers (the MCP/API
// edge) can report success/failure directly to the caller instead of
// polling last_error.
func (e *Engine) TestFire(ctx context.Context, id int64) error {
	rule, ok := e.ruleByID(id)
	if !ok {
		return fmt.Errorf("alerts: rule %d not found", id)
	}

	job := fireJob{
		rule:      rule,
		issueID:   0,
		title:     "Sample issue",
		severity:  core.SeverityError,
		eventTime: time.Now(),
		test:      true,
	}
	if rule.Kind == core.AlertThreshold {
		job.eventCount = rule.N
		job.windowMinutes = rule.WindowMinutes
	}

	payload := buildPayload(job)
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	deliveryErr := e.attemptDelivery(ctx, rule, body)
	e.recordResult(rule.ID, deliveryErr)
	return deliveryErr
}
