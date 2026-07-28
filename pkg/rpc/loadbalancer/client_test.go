package loadbalancer

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newTestClient(t *testing.T, config *Config) *Client {
	t.Helper()

	// Lazy dial: no connection is attempted until an RPC is made, which is
	// exactly the state a client is in when the server is unreachable.
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to build client conn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &Client{conn: conn, config: config}
}

// A call that never returns on its own must still be bounded by Config.Timeout.
// Calls are made with WaitForReady(true), so before this was enforced an
// unreachable server blocked for the lifetime of the caller's context.
func TestRetryOnFailureBoundsEachAttempt(t *testing.T) {
	c := newTestClient(t, &Config{
		ServerAddr: "127.0.0.1:1",
		Timeout:    150 * time.Millisecond,
		RetryMax:   2,
		RetryDelay: time.Millisecond,
	})

	var attempts int
	start := time.Now()
	err := c.retryOnFailure(context.Background(), "Blocking", func(ctx context.Context) error {
		attempts++
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error once every attempt timed out")
	}
	if attempts != c.config.RetryMax+1 {
		t.Errorf("expected %d attempts, got %d", c.config.RetryMax+1, attempts)
	}
	// 3 attempts x 150ms plus backoff, with generous slack for slow CI.
	if elapsed > 3*time.Second {
		t.Errorf("retryOnFailure took %v; per-attempt deadline not applied", elapsed)
	}
}

// The caller's own deadline still wins, and is reported as such rather than
// being retried against.
func TestRetryOnFailureHonoursCallerContext(t *testing.T) {
	c := newTestClient(t, &Config{
		ServerAddr: "127.0.0.1:1",
		Timeout:    10 * time.Second,
		RetryMax:   5,
		RetryDelay: time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.retryOnFailure(ctx, "Blocking", func(innerCtx context.Context) error {
		<-innerCtx.Done()
		return status.FromContextError(innerCtx.Err()).Err()
	})
	elapsed := time.Since(start)

	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("retryOnFailure took %v; caller context not honoured", elapsed)
	}
}

func TestCalculateBackoffIsCapped(t *testing.T) {
	c := &Client{config: &Config{RetryDelay: time.Second}}
	for attempt := 1; attempt <= 10; attempt++ {
		if delay := c.calculateBackoff(attempt); delay > 5*time.Second {
			t.Errorf("attempt %d: backoff %v exceeds the 5s cap", attempt, delay)
		}
	}
}
