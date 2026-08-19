package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestShutdownWaitsForInFlightRecording(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	cfg := config.Load()
	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer rdb.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), rdb, log)

	evt := ingest.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/rec.wav",
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording to be processed by the time Shutdown returned")
	}
}

func TestShutdownRespectsDeadline(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()

	cfg := config.Load()
	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	defer rdb.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), rdb, log)

	shutdownCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	if err := svc.Shutdown(shutdownCtx); err == nil {
		t.Fatal("expected Shutdown to return the expired context's error")
	}
}
