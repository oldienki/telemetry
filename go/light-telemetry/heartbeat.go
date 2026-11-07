package telemetry

import (
	"encoding/json"
	"errors"
	"net"
	"time"
)

// isTimeout reports whether err is a socket read-deadline expiry.
func isTimeout(err error) bool {
	var nerr net.Error
	return errors.As(err, &nerr) && nerr.Timeout()
}

// heartbeat is the server's reverse heartbeat payload: {"type":"hb","srv_ts":…}.
type heartbeat struct {
	Type  string `json:"type"`
	SrvTS uint64 `json:"srv_ts"`
}

// watch reads datagrams on the client's connected socket (recv is already
// filtered to the server address by DialUDP) and flips the degraded flag
// when heartbeats stop arriving. Exits when stop closes or the socket
// errors — watching is best-effort.
func (c *Client) watch() {
	defer close(c.watcherDone)
	timeout := c.cfg.HeartbeatTimeout
	// The read deadline must stay well below the liveness window or a single
	// blocked read would itself exceed timeout and flap degraded right
	// after a fresh heartbeat.
	readTimeout := min(max(timeout/4, 20*time.Millisecond), 200*time.Millisecond)
	var lastHB time.Time
	seenHB := false
	buf := make([]byte, 2048)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))
		n, err := c.conn.Read(buf)
		if err == nil {
			var hb heartbeat
			if json.Unmarshal(buf[:n], &hb) == nil && hb.Type == "hb" {
				lastHB = time.Now()
				seenHB = true
			}
		} else if !isTimeout(err) {
			return // real socket error → give up watching
		}
		// A client that never saw a heartbeat stays healthy — degradation is
		// proof of loss, not absence.
		c.degraded.Store(seenHB && time.Since(lastHB) > timeout)
	}
}
