package loadbalancer

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	loadbalancerv1 "kubevirt.io/cloud-provider-kubevirt/pkg/rpc/loadbalancer/gen"
)

type Config struct {
	ServerAddr string
	// Timeout bounds a single RPC attempt. Each retry gets a fresh Timeout.
	Timeout    time.Duration
	RetryMax   int
	RetryDelay time.Duration
	DialOpts   []grpc.DialOption
	ApiKey     string
}

type apiKeyCreds struct {
	apiKey string
}

func (c apiKeyCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.apiKey,
	}, nil
}

func (c apiKeyCreds) RequireTransportSecurity() bool {
	return false
}

type Client struct {
	conn   *grpc.ClientConn
	client loadbalancerv1.LoadBalancerServiceClient
	config *Config
}

func NewClient(ctx context.Context, config *Config) (*Client, error) {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.RetryMax == 0 {
		config.RetryMax = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	}

	if config.DialOpts == nil {
		config.DialOpts = []grpc.DialOption{
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		}
	}

	if config.ApiKey != "" {
		config.DialOpts = append(config.DialOpts, grpc.WithPerRPCCredentials(apiKeyCreds{apiKey: config.ApiKey}))
	}

	target, err := normalizeServerAddr(config.ServerAddr)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, target, config.DialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC server: %w", err)
	}

	client := loadbalancerv1.NewLoadBalancerServiceClient(conn)

	return &Client{
		conn:   conn,
		client: client,
		config: config,
	}, nil
}

// normalizeServerAddr turns a configured address into a target gRPC can dial.
// gRPC expects "host:port"; given "http://host:port" it treats the whole string
// as an address and the dial fails with "too many colons in address", which
// says nothing about the real problem. An http:// URL is a natural thing to
// write in a config file, so accept it and strip it down.
//
// Anything carrying a scheme gRPC understands itself - dns:, unix:,
// passthrough: - is handed through untouched.
func normalizeServerAddr(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("RPC server address is empty")
	}
	if !strings.Contains(addr, "://") {
		return addr, nil
	}

	u, err := url.Parse(addr)
	if err != nil {
		return "", fmt.Errorf("invalid RPC server address %q: %w", addr, err)
	}

	switch u.Scheme {
	case "http":
	case "https":
		// Silently dialing plaintext would betray what the address asked for.
		return "", fmt.Errorf("invalid RPC server address %q: TLS is not supported, use http:// or host:port", addr)
	default:
		return addr, nil
	}

	// Hostname/Port rather than Host, so a bracketed IPv6 literal survives.
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("invalid RPC server address %q: no host", addr)
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	return net.JoinHostPort(host, port), nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) isConnectionReady() bool {
	return c.conn != nil && c.conn.GetState() == connectivity.Ready
}

func (c *Client) waitForReady(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("connection is not initialized")
	}

	for {
		state := c.conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("connection is shutdown")
		}
		c.conn.ResetConnectBackoff()
		connState := c.conn.WaitForStateChange(ctx, state)
		if !connState {
			return ctx.Err()
		}
	}
}

func (c *Client) retryOnFailure(ctx context.Context, operation string, fn func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt <= c.config.RetryMax; attempt++ {
		if attempt > 0 {
			delay := c.calculateBackoff(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		// Every attempt gets its own deadline. Calls are made with
		// WaitForReady(true), so without one they block for as long as the
		// caller's context lives - which for the service controller is
		// effectively forever, stalling reconciliation whenever the server
		// is unreachable.
		attemptCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)

		if !c.isConnectionReady() {
			if err := c.waitForReady(attemptCtx); err != nil {
				cancel()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				lastErr = fmt.Errorf("connection not ready: %w", err)
				continue
			}
		}

		err := fn(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}

		// The caller gave up (or its deadline passed); retrying is pointless
		// and would misreport the reason.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		st, ok := status.FromError(err)
		if !ok {
			lastErr = fmt.Errorf("%s: unknown error type: %w", operation, err)
			continue
		}

		switch st.Code() {
		case codes.OK:
			return nil
		case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
			lastErr = err
			continue
		case codes.NotFound, codes.AlreadyExists, codes.InvalidArgument, codes.PermissionDenied:
			return err
		default:
			lastErr = err
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, c.config.RetryMax+1, lastErr)
}

func (c *Client) calculateBackoff(attempt int) time.Duration {
	base := c.config.RetryDelay
	maxDelay := 5 * time.Second

	backoff := base * time.Duration(1<<uint(attempt))
	jitter := time.Duration(rand.Int63n(int64(backoff / 2)))

	delay := backoff + jitter
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

func (c *Client) CreateLoadBalancer(ctx context.Context, req *loadbalancerv1.CreateLoadBalancerRequest) (*loadbalancerv1.CreateLoadBalancerResponse, error) {
	var resp *loadbalancerv1.CreateLoadBalancerResponse
	err := c.retryOnFailure(ctx, "CreateLoadBalancer", func(innerCtx context.Context) error {
		r, err := c.client.CreateLoadBalancer(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) GetLoadBalancer(ctx context.Context, req *loadbalancerv1.GetLoadBalancerRequest) (*loadbalancerv1.GetLoadBalancerResponse, error) {
	var resp *loadbalancerv1.GetLoadBalancerResponse
	err := c.retryOnFailure(ctx, "GetLoadBalancer", func(innerCtx context.Context) error {
		r, err := c.client.GetLoadBalancer(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) ListLoadBalancers(ctx context.Context, req *loadbalancerv1.ListLoadBalancersRequest) (*loadbalancerv1.ListLoadBalancersResponse, error) {
	var resp *loadbalancerv1.ListLoadBalancersResponse
	err := c.retryOnFailure(ctx, "ListLoadBalancers", func(innerCtx context.Context) error {
		r, err := c.client.ListLoadBalancers(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) DeleteLoadBalancer(ctx context.Context, req *loadbalancerv1.DeleteLoadBalancerRequest) (*loadbalancerv1.DeleteLoadBalancerResponse, error) {
	var resp *loadbalancerv1.DeleteLoadBalancerResponse
	err := c.retryOnFailure(ctx, "DeleteLoadBalancer", func(innerCtx context.Context) error {
		r, err := c.client.DeleteLoadBalancer(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) UpdateLoadBalancer(ctx context.Context, req *loadbalancerv1.UpdateLoadBalancerRequest) (*loadbalancerv1.UpdateLoadBalancerResponse, error) {
	var resp *loadbalancerv1.UpdateLoadBalancerResponse
	err := c.retryOnFailure(ctx, "UpdateLoadBalancer", func(innerCtx context.Context) error {
		r, err := c.client.UpdateLoadBalancer(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) AllocateFloatingIp(ctx context.Context, req *loadbalancerv1.AllocateFloatingIpRequest) (*loadbalancerv1.AllocateFloatingIpResponse, error) {
	var resp *loadbalancerv1.AllocateFloatingIpResponse
	err := c.retryOnFailure(ctx, "AllocateFloatingIp", func(innerCtx context.Context) error {
		r, err := c.client.AllocateFloatingIp(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func (c *Client) ReleaseFloatingIp(ctx context.Context, req *loadbalancerv1.ReleaseFloatingIpRequest) (*loadbalancerv1.ReleaseFloatingIpResponse, error) {
	var resp *loadbalancerv1.ReleaseFloatingIpResponse
	err := c.retryOnFailure(ctx, "ReleaseFloatingIp", func(innerCtx context.Context) error {
		r, err := c.client.ReleaseFloatingIp(innerCtx, req, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

func ToRPCError(err error) string {
	if err == nil {
		return ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return err.Error()
	}

	switch st.Code() {
	case codes.Unavailable:
		return "Load balancer service temporarily unavailable"
	case codes.DeadlineExceeded:
		return "Load balancer operation timed out"
	case codes.NotFound:
		return "Load balancer not found"
	case codes.AlreadyExists:
		return "Load balancer already exists"
	case codes.InvalidArgument:
		return "Invalid load balancer configuration"
	case codes.PermissionDenied:
		return "Permission denied to manage load balancer"
	default:
		return st.Message()
	}
}

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return true
	}

	switch st.Code() {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
