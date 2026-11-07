// Discovery of the server's telemetry.toml so a localhost client can
// adopt the daemon's udp_bind and heartbeat_ms without manual wiring.
//
// Same search order as the server: explicit path, else
// /etc/telemetry.toml, else $HOME/.config/telemetry.toml; the first
// existing file wins. Only the keys the client consumes are read —
// server-only keys are ignored.
package telemetry

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	etcPath             = "/etc/telemetry.toml"
	userPath            = ".config/telemetry.toml"
	defaultUDPBind      = "0.0.0.0:9876"
	defaultHeartbeatMs  = 1000
	heartbeatMultiplier = 3 // liveness window = N × heartbeat interval
)

// ServerConfig holds the server keys a client consumes.
type ServerConfig struct {
	UDPBind     string // daemon UDP ingest bind, e.g. "0.0.0.0:9876"
	HeartbeatMs int64  // daemon heartbeat interval in milliseconds
}

// defaultPaths returns the search candidates in order.
func defaultPaths() []string {
	paths := []string{etcPath}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, userPath))
	}
	return paths
}

// LoadServerConfig reads an explicit config path (must exist), else the
// first existing default path, else the zero value.
func LoadServerConfig(explicit string) (ServerConfig, string, error) {
	if explicit != "" {
		cfg, err := parseServerFile(explicit)
		return cfg, explicit, err
	}
	for _, cand := range defaultPaths() {
		if st, err := os.Stat(cand); err == nil && !st.IsDir() {
			cfg, err := parseServerFile(cand)
			return cfg, cand, err
		}
	}
	return ServerConfig{}, "", nil
}

// parseServerFile extracts the client-relevant scalars from a
// telemetry.toml. Only udp_bind and heartbeat_ms are recognized; lines
// that do not parse are ignored — discovery must never crash a client.
func parseServerFile(path string) (ServerConfig, error) {
	var cfg ServerConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(stripTOMLComment(raw))
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "udp_bind":
			cfg.UDPBind = strings.Trim(strings.TrimSpace(v), `"`)
		case "heartbeat_ms":
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				cfg.HeartbeatMs = n
			}
		}
	}
	return cfg, nil
}

// stripTOMLComment removes a '#' comment outside quoted strings.
func stripTOMLComment(line string) string {
	inStr := false
	for i, c := range line {
		switch {
		case c == '"':
			inStr = !inStr
		case c == '#' && !inStr:
			return line[:i]
		}
	}
	return line
}

// localize maps a bind address to a send target: wildcard → same-family
// loopback.
func localize(bind string) string {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return "127.0.0.1:9876"
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
	}
	return net.JoinHostPort(host, port)
}
