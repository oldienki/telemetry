package telemetry

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCfg(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "telemetry.toml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadServerConfigExplicit(t *testing.T) {
	p := writeCfg(t, `
# daemon settings
udp_bind = "0.0.0.0:44444"   # ingest
heartbeat_ms = 200
storage = "sqlite"
db_url = "sqlite:x.db"
`)
	cfg, src, err := LoadServerConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if src != p {
		t.Errorf("src = %q", src)
	}
	if cfg.UDPBind != "0.0.0.0:44444" {
		t.Errorf("udp_bind = %q", cfg.UDPBind)
	}
	if cfg.HeartbeatMs != 200 {
		t.Errorf("heartbeat_ms = %d", cfg.HeartbeatMs)
	}
}

func TestLoadServerConfigMissingExplicitErrors(t *testing.T) {
	if _, _, err := LoadServerConfig("/nonexistent/x.toml"); err == nil {
		t.Fatal("expected error")
	}
}

func TestLocalize(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:9876":    "127.0.0.1:9876",
		"[::]:9876":       "[::1]:9876",
		"192.168.1.5:999": "192.168.1.5:999",
	}
	for in, want := range cases {
		if got := localize(in); got != want {
			t.Errorf("localize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewLocalClientFromFile(t *testing.T) {
	p := writeCfg(t, "udp_bind = \"0.0.0.0:0\"\nheartbeat_ms = 150\n")
	c, err := NewLocalClient("h", "c", p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.cfg.ServerAddr != "127.0.0.1:0" {
		t.Errorf("addr = %q", c.cfg.ServerAddr)
	}
	if c.cfg.HeartbeatTimeout != 450*time.Millisecond {
		t.Errorf("timeout = %v", c.cfg.HeartbeatTimeout)
	}
	if !c.watcherStarted {
		t.Error("watcher should be started for derived timeout")
	}
}

func TestNewLocalClientDefaults(t *testing.T) {
	// Point HOME at an empty dir so no user config is found.
	t.Setenv("HOME", t.TempDir())
	if _, err := os.Stat("/etc/telemetry.toml"); err == nil {
		t.Skip("system config exists; cannot assert defaults")
	}
	c, err := NewLocalClient("h", "c", "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.cfg.ServerAddr != "127.0.0.1:9876" {
		t.Errorf("addr = %q", c.cfg.ServerAddr)
	}
	if c.cfg.HeartbeatTimeout != 3*time.Second {
		t.Errorf("timeout = %v", c.cfg.HeartbeatTimeout)
	}
}
