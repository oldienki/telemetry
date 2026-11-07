// Package telemetry is the LightTelemetry UDP fire-and-forget client.
//
// Design contract (shared across the Rust, Python, and Go clients):
//   - Emit serializes one packet and sends one datagram. No buffering, no
//     retries — telemetry must never block the host.
//   - A background goroutine watches reverse heartbeats from the server.
//     When they stop, the client degrades and Emit drops before serializing.
//   - Oversized packets are truncated (payload -> {"truncated": true}).
package telemetry

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Client.
type Config struct {
	// ServerAddr is the ingest endpoint ("host:port").
	ServerAddr string
	// HostID identifies this emitter ("id" field).
	HostID string
	// HostClass classifies this emitter ("class" field).
	HostClass string
	// UseMsgpack selects MessagePack wire format (default JSON).
	UseMsgpack bool
	// HeartbeatTimeout is the liveness window for the reverse-heartbeat
	// watcher. <= 0 disables the watcher entirely.
	HeartbeatTimeout time.Duration
}

// ClientStats counts client-side outcomes (diagnostics and tests).
type ClientStats struct {
	Encoded   atomic.Int64 // packets that entered serialization
	Sent      atomic.Int64 // datagrams handed to the socket
	Dropped   atomic.Int64 // emissions dropped while degraded
	Truncated atomic.Int64 // payloads replaced by the truncation marker
	Errors    atomic.Int64 // encode or send failures
}

// Client is a single UDP connection plus an optional heartbeat watcher.
type Client struct {
	conn           *net.UDPConn
	cfg            Config
	degraded       atomic.Bool
	stats          ClientStats
	stop           chan struct{}
	watcherDone    chan struct{}
	watcherStarted bool
	closeOnce      sync.Once
}

// NewClient opens a UDP connection toward cfg.ServerAddr and starts the
// heartbeat watcher when HeartbeatTimeout > 0.
func NewClient(cfg Config) (*Client, error) {
	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("telemetry: ServerAddr is required")
	}
	raddr, err := net.ResolveUDPAddr("udp", cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resolve %q: %w", cfg.ServerAddr, err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("telemetry: dial %q: %w", cfg.ServerAddr, err)
	}
	c := &Client{
		conn: conn,
		cfg:  cfg,
		stop: make(chan struct{}),
	}
	if cfg.HeartbeatTimeout > 0 {
		c.watcherDone = make(chan struct{})
		c.watcherStarted = true
		go c.watch()
	}
	return c, nil
}

// Close stops the watcher and closes the socket. Idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.stop)
		if c.watcherStarted {
			<-c.watcherDone
		}
		_ = c.conn.Close()
	})
	return nil
}

// Degraded reports whether the server is currently unreachable.
func (c *Client) Degraded() bool { return c.degraded.Load() }

// Stats returns the client counters.
func (c *Client) Stats() *ClientStats { return &c.stats }

// LocalAddr returns the socket's local address (used by tests).
func (c *Client) LocalAddr() *net.UDPAddr {
	addr, _ := c.conn.LocalAddr().(*net.UDPAddr)
	return addr
}

// Emit sends one event. Drops silently when degraded; never retries and
// never reports errors upward — callers must not pay for telemetry failures.
func (c *Client) Emit(topic string, payload any) {
	if c.Degraded() {
		c.stats.Dropped.Add(1)
		return
	}
	c.stats.Encoded.Add(1)
	data, truncated, err := encode(Packet{
		Topic:   topic,
		ID:      c.cfg.HostID,
		Class:   c.cfg.HostClass,
		TS:      nowMicros(),
		Payload: payload,
	}, c.cfg.UseMsgpack)
	if err != nil {
		c.stats.Errors.Add(1)
		return
	}
	if truncated {
		c.stats.Truncated.Add(1)
	}
	if _, err := c.conn.Write(data); err != nil {
		c.stats.Errors.Add(1)
		return
	}
	c.stats.Sent.Add(1)
}
