package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func mustListen(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustClient(t *testing.T, server *net.UDPConn) *Client {
	t.Helper()
	c, err := NewClient(Config{
		ServerAddr: server.LocalAddr().String(),
		HostID:     "test-host",
		HostClass:  "test-class",
		// HeartbeatTimeout 0 → watcher off for most tests.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func recvPacket(t *testing.T, server *net.UDPConn) map[string]any {
	t.Helper()
	buf := make([]byte, 8192)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := server.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(buf[:n], &v); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	return v
}

// goldenPacket mirrors spec/test-vectors/packet_sample.json.
func goldenPacket() Packet {
	return Packet{
		Topic: "db.query",
		ID:    "worker-node-04",
		Class: "storage",
		TS:    1772874900123456,
		Payload: map[string]any{
			"query":       "SELECT * FROM users WHERE id = $1",
			"duration_ms": 1.42,
			"status":      "ok",
		},
	}
}

func goldenJSON(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../spec/test-vectors/packet_sample.json")
	if err != nil {
		t.Fatalf("golden: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("golden parse: %v", err)
	}
	return v
}

func TestJSONEncodingMatchesGoldenVector(t *testing.T) {
	data, truncated, err := encode(goldenPacket(), false)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if truncated {
		t.Fatal("golden packet must not truncate")
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(goldenJSON(t))
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("mismatch:\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

func TestMsgpackEncodingMatchesGoldenVector(t *testing.T) {
	data, truncated, err := encode(goldenPacket(), true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if truncated {
		t.Fatal("golden packet must not truncate")
	}
	var got map[string]any
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("msgpack decode: %v", err)
	}
	want := goldenJSON(t)
	for _, k := range []string{"topic", "id", "class"} {
		if got[k] != want[k] {
			t.Fatalf("field %s: got %v want %v", k, got[k], want[k])
		}
	}
	// ts decodes as int64 via msgpack but float64 via JSON — compare numerically.
	toI64 := func(v any) int64 {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case uint64:
			return int64(n)
		}
		return -1
	}
	if toI64(got["ts"]) != toI64(want["ts"]) {
		t.Fatalf("ts: got %v want %v", got["ts"], want["ts"])
	}
}

func TestOversizedPayloadTruncates(t *testing.T) {
	p := goldenPacket()
	p.Payload = map[string]any{"big": strings.Repeat("x", 2000)}
	for _, mp := range []bool{false, true} {
		data, truncated, err := encode(p, mp)
		if err != nil {
			t.Fatalf("encode(msgpack=%v): %v", mp, err)
		}
		if !truncated {
			t.Fatalf("msgpack=%v: expected truncation", mp)
		}
		if len(data) > MTUSafe {
			t.Fatalf("msgpack=%v: %d bytes > %d", mp, len(data), MTUSafe)
		}
	}
}

func TestExactMTUBoundaryNotTruncated(t *testing.T) {
	p := goldenPacket()
	for n := 0; n <= MTUSafe; n++ {
		p.Payload = map[string]any{"pad": strings.Repeat("x", n)}
		data, truncated, err := encode(p, false)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if len(data) == MTUSafe && !truncated {
			p.Payload = map[string]any{"pad": strings.Repeat("x", n+1)}
			data, truncated, err := encode(p, false)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !truncated || len(data) > MTUSafe {
				t.Fatal("one byte over exact fit must truncate")
			}
			return
		}
	}
	t.Fatal("no exact-fit payload size found")
}

func TestUnicodePayloadRoundTrips(t *testing.T) {
	p := goldenPacket()
	p.Payload = map[string]any{"msg": "héllo  🚀"}
	data, _, err := encode(p, false)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["payload"].(map[string]any)["msg"] != "héllo  🚀" {
		t.Fatal("unicode corrupted")
	}
}

func TestEmitSendsDatagram(t *testing.T) {
	server := mustListen(t)
	c := mustClient(t, server)
	c.Emit("t", map[string]any{"k": 1})
	v := recvPacket(t, server)
	if v["topic"] != "t" || v["id"] != "test-host" || v["class"] != "test-class" {
		t.Fatalf("bad packet: %v", v)
	}
	if v["ts"].(float64) <= 0 {
		t.Fatal("ts must be set")
	}
	if c.Stats().Sent.Load() != 1 || c.Stats().Encoded.Load() != 1 {
		t.Fatalf("stats: %+v", c.Stats())
	}
}

func TestEmitToUnreachableServerDoesNotPanic(t *testing.T) {
	c, err := NewClient(Config{ServerAddr: "127.0.0.1:9", HostID: "h", HostClass: "c"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	c.Emit("t", nil)
	if c.Stats().Sent.Load() != 1 {
		t.Fatal("UDP send to discard port should succeed")
	}
}

func TestDegradedDropsBeforeSerialization(t *testing.T) {
	server := mustListen(t)
	c := mustClient(t, server)
	c.degraded.Store(true)
	encoded := c.Stats().Encoded.Load()
	c.Emit("t", map[string]any{"expensive": "x"})
	if c.Stats().Encoded.Load() != encoded {
		t.Fatal("serializer must not run while degraded")
	}
	if c.Stats().Dropped.Load() != 1 || c.Stats().Sent.Load() != 0 {
		t.Fatalf("stats: %+v", c.Stats())
	}
}

func TestCloseIdempotent(t *testing.T) {
	server := mustListen(t)
	c := mustClient(t, server)
	if err := c.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}
}

func TestNewClientRequiresServerAddr(t *testing.T) {
	if _, err := NewClient(Config{HostID: "h", HostClass: "c"}); err == nil {
		t.Fatal("expected error for empty ServerAddr")
	}
}

func TestWatcherDegradeAndRecover(t *testing.T) {
	server := mustListen(t)
	c, err := NewClient(Config{
		ServerAddr:       server.LocalAddr().String(),
		HostID:           "h",
		HostClass:        "c",
		HeartbeatTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	hb := []byte(`{"type":"hb","srv_ts":1}`)
	clientAddr := c.LocalAddr()

	// Heartbeats → healthy.
	for i := 0; i < 4; i++ {
		if _, err := server.WriteToUDP(hb, clientAddr); err != nil {
			t.Fatalf("hb: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond)
	if c.Degraded() {
		t.Fatal("should be healthy with heartbeats")
	}

	// Silence → degraded.
	time.Sleep(400 * time.Millisecond)
	if !c.Degraded() {
		t.Fatal("should be degraded after silence")
	}

	// Resume → recover (assert inside the liveness window).
	for i := 0; i < 3; i++ {
		if _, err := server.WriteToUDP(hb, clientAddr); err != nil {
			t.Fatalf("hb: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond)
	if c.Degraded() {
		t.Fatal("should recover after heartbeat resumes")
	}
}

// driveDegraded sends one heartbeat then stays silent until the client
// reports degraded.
func driveDegraded(t *testing.T, server *net.UDPConn, c *Client, timeout time.Duration) {
	t.Helper()
	hb := []byte(`{"type":"hb","srv_ts":1}`)
	if _, err := server.WriteToUDP(hb, c.LocalAddr()); err != nil {
		t.Fatalf("hb: %v", err)
	}
	deadline := time.Now().Add(timeout * 6)
	for !c.Degraded() {
		if time.Now().After(deadline) {
			t.Fatal("client never degraded")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func watchedClient(t *testing.T, server *net.UDPConn, useMsgpack bool) *Client {
	t.Helper()
	c, err := NewClient(Config{
		ServerAddr:       server.LocalAddr().String(),
		HostID:           "h",
		HostClass:        "c",
		UseMsgpack:       useMsgpack,
		HeartbeatTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDegradedWatcherSendsSessionProbe(t *testing.T) {
	server := mustListen(t)
	c := watchedClient(t, server, false)
	driveDegraded(t, server, c, 150*time.Millisecond)

	v := recvPacket(t, server)
	if v["topic"] != "_session" {
		t.Fatalf("expected _session probe, got %v", v["topic"])
	}
	if v["id"] != "h" || v["class"] != "c" {
		t.Fatalf("probe identity wrong: %v", v)
	}
	if ts, ok := v["ts"].(float64); !ok || ts <= 0 {
		t.Fatalf("probe ts wrong: %v", v["ts"])
	}
	if v["payload"] != nil {
		t.Fatalf("probe payload must be null, got %v", v["payload"])
	}
}

func TestProbesAreRateLimitedToOnePerTimeout(t *testing.T) {
	server := mustListen(t)
	c := watchedClient(t, server, false)
	driveDegraded(t, server, c, 150*time.Millisecond)

	// Drain probes emitted while degrading, then count fresh probes over
	// ~2.5 timeout windows → at most 3.
	buf := make([]byte, 2048)
	_ = server.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	for {
		if _, _, err := server.ReadFromUDP(buf); err != nil {
			break
		}
	}
	count := 0
	end := time.Now().Add(380 * time.Millisecond)
	for time.Now().Before(end) {
		_ = server.SetReadDeadline(time.Now().Add(80 * time.Millisecond))
		n, _, err := server.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal(buf[:n], &v); err != nil {
			t.Fatalf("probe decode: %v", err)
		}
		if v["topic"] != "_session" {
			t.Fatalf("unexpected datagram: %v", v["topic"])
		}
		count++
	}
	if count < 1 || count > 3 {
		t.Fatalf("expected 1..3 probes, got %d", count)
	}
}

func TestHeartbeatReplyToProbeRecoversClient(t *testing.T) {
	server := mustListen(t)
	c := watchedClient(t, server, false)
	driveDegraded(t, server, c, 150*time.Millisecond)

	// Server sees a probe and answers with a heartbeat.
	buf := make([]byte, 2048)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, from, err := server.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("recv probe: %v", err)
	}
	if _, err := server.WriteToUDP([]byte(`{"type":"hb","srv_ts":2}`), from); err != nil {
		t.Fatalf("hb: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for c.Degraded() {
		if time.Now().After(deadline) {
			t.Fatal("client never recovered")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMsgpackProbeDecodes(t *testing.T) {
	server := mustListen(t)
	c := watchedClient(t, server, true)
	driveDegraded(t, server, c, 150*time.Millisecond)

	buf := make([]byte, 2048)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := server.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("recv probe: %v", err)
	}
	if buf[0] == '{' {
		t.Fatal("probe must be msgpack, not JSON")
	}
	var v map[string]any
	if err := msgpack.Unmarshal(buf[:n], &v); err != nil {
		t.Fatalf("msgpack decode: %v", err)
	}
	if v["topic"] != "_session" {
		t.Fatalf("expected _session probe, got %v", v["topic"])
	}
}

// -- slog handler ---------------------------------------------------------

func mustLogger(t *testing.T, server *net.UDPConn) (*Client, *slog.Logger) {
	t.Helper()
	c := mustClient(t, server)
	return c, slog.New(NewSlogHandler(c))
}

func TestHandlerEmitsRecordWithAttrs(t *testing.T) {
	server := mustListen(t)
	_, logger := mustLogger(t, server)
	logger.Info("login ok", "user", "alice", "answer", 42)
	v := recvPacket(t, server)
	if v["topic"] != "logger" {
		t.Fatalf("topic: %v", v["topic"])
	}
	p := v["payload"].(map[string]any)
	if p["level"] != "INFO" || p["message"] != "login ok" || p["user"] != "alice" {
		t.Fatalf("payload: %v", p)
	}
	if p["answer"].(float64) != 42 {
		t.Fatalf("attr answer: %v", p["answer"])
	}
}

func TestHandlerNestedGroupAttrs(t *testing.T) {
	server := mustListen(t)
	_, logger := mustLogger(t, server)
	logger.Info("req", slog.Group("http", "method", "GET", "status", 200))
	v := recvPacket(t, server)
	p := v["payload"].(map[string]any)
	http := p["http"].(map[string]any)
	if http["method"] != "GET" || http["status"].(float64) != 200 {
		t.Fatalf("group: %v", http)
	}
}

func TestHandlerWithAttrsAndGroup(t *testing.T) {
	server := mustListen(t)
	c := mustClient(t, server)
	h := NewSlogHandler(c).WithAttrs([]slog.Attr{slog.String("service", "api")}).WithGroup("outer")
	logger := slog.New(h)
	logger.Info("x", "k", "v")
	v := recvPacket(t, server)
	p := v["payload"].(map[string]any)
	outer := p["outer"].(map[string]any)
	if outer["service"] != "api" || outer["k"] != "v" {
		t.Fatalf("grouped attrs: %v", p)
	}
}

func TestHandlerEnabledAndLevel(t *testing.T) {
	server := mustListen(t)
	c := mustClient(t, server)
	h := NewSlogHandler(c).WithMinLevel(slog.LevelWarn)
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("info must be filtered below warn")
	}
	logger := slog.New(h)
	logger.Info("filtered")
	logger.Warn("kept")
	v := recvPacket(t, server)
	if v["payload"].(map[string]any)["level"] != "WARN" {
		t.Fatalf("level: %v", v["payload"])
	}
}

func TestHandlerSurvivesUnreachableServer(t *testing.T) {
	c, err := NewClient(Config{ServerAddr: "127.0.0.1:9", HostID: "h", HostClass: "c"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	slog.New(NewSlogHandler(c)).Error("nothing hears this")
}
