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

// The configured address is written by hand in a ConfigMap, so it turns up both
// as a bare host:port and as an http:// URL. gRPC only understands the former:
// handed "http://10.2.5.250:9000" it dials that whole string and fails with
// "too many colons in address".
func TestNormalizeServerAddr(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"host and port", "10.2.5.250:9000", "10.2.5.250:9000"},
		{"http url", "http://10.2.5.250:9000", "10.2.5.250:9000"},
		{"http url with trailing slash", "http://10.2.5.250:9000/", "10.2.5.250:9000"},
		{"http url without port", "http://lb.example.com", "lb.example.com:80"},
		{"surrounding whitespace", "  http://10.2.5.250:9000\n", "10.2.5.250:9000"},
		{"dns name and port", "lb.example.com:9000", "lb.example.com:9000"},
		{"ipv6 url", "http://[fd00::1]:9000", "[fd00::1]:9000"},
		{"grpc resolver target", "dns:///lb.example.com:9000", "dns:///lb.example.com:9000"},
		{"unix socket", "unix:///var/run/lb.sock", "unix:///var/run/lb.sock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeServerAddr(tc.in)
			if err != nil {
				t.Fatalf("normalizeServerAddr(%q) returned error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("normalizeServerAddr(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeServerAddrRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"empty", "  "},
		// Stripping the scheme and dialing plaintext anyway would quietly
		// deliver the opposite of what the address asked for.
		{"https", "https://10.2.5.250:9000"},
		{"no host", "http://"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := normalizeServerAddr(tc.in); err == nil {
				t.Errorf("normalizeServerAddr(%q) = %q, want an error", tc.in, got)
			}
		})
	}
}
