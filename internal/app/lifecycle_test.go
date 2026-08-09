package app

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store/sqlite"
)

// TestStopWaitsForAlertDelivery pins register's OnStop ordering (see
// lifecycle.go): pipe.Drain must run before the alerts worker's ctx is
// canceled, and RequireStop must block until that worker's in-flight
// delivery actually completes — never leaving the goroutine to finish (or
// abandon its POST) after Stop has already returned. It does this for
// real: seed a new_issue alert rule pointed at a local httptest server,
// push one log through the real ingest edge so the rule fires, then call
// RequireStop immediately and assert the webhook was received by the time
// it returns.
func TestStopWaitsForAlertDelivery(t *testing.T) {
	var delivered atomic.Bool
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	cfg := testConfig(t, "test-admin-password")

	var db *sqlite.DB
	var engine *alerts.Engine
	app := fxtest.New(t,
		Module,
		fx.Replace(cfg),
		fx.Populate(&db, &engine),
	)
	app.RequireStart()

	ctx := context.Background()
	proj, err := db.CreateProject(ctx, "proj", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	ingestKey, err := db.MintKey(ctx, proj.ID, "ingest")
	if err != nil {
		t.Fatalf("MintKey: %v", err)
	}
	// Go through the engine's own Upsert, not db.UpsertAlertRule directly:
	// the engine only sees rules written since its last Load, and Upsert
	// is what triggers that reload (see alerts.Engine.Upsert) — a
	// store-level write here would leave register's OnStart-time Load as
	// the last one the engine ever saw, i.e. no rules at all.
	if _, err := engine.Upsert(ctx, core.AlertRule{
		ProjectID: proj.ID,
		Name:      "on new issue",
		Kind:      core.AlertNewIssue,
		URL:       webhook.URL,
		Enabled:   true,
	}); err != nil {
		t.Fatalf("engine.Upsert: %v", err)
	}

	body := []byte(`[{"severity":"error","message":"boom"}]`)
	req, err := http.NewRequest(http.MethodPost, "http://"+cfg.ListenAddr+"/api/v1/ingest", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+ingestKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest status = %d, want 200/202", resp.StatusCode)
	}

	// No sleep/poll here on purpose: the point of this test is that
	// RequireStop itself (via pipe.Drain then <-alertsDone in OnStop)
	// waits out the pipeline flush, the rule evaluation, and the webhook
	// delivery — so by the time it returns, delivered must already be
	// true with no help from the test.
	app.RequireStop()

	if !delivered.Load() {
		t.Fatal("app stopped before the alert webhook was delivered — OnStop must wait for the alerts worker to drain, see lifecycle.go's cancelAlerts/<-alertsDone ordering")
	}
}
