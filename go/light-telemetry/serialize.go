package telemetry

import (
	"encoding/json"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// MTUSafe is the maximum wire size before the payload is replaced by a
// truncation marker. Sized to stay under a single IPv4 UDP datagram over
// typical MTUs.
const MTUSafe = 1432

// Packet is one telemetry event on the wire; field names match spec/schemas.
type Packet struct {
	Topic   string `json:"topic" msgpack:"topic"`
	ID      string `json:"id" msgpack:"id"`
	Class   string `json:"class" msgpack:"class"`
	TS      uint64 `json:"ts" msgpack:"ts"`
	Payload any    `json:"payload" msgpack:"payload"`
}

// nowMicros returns the current UNIX epoch time in microseconds.
func nowMicros() uint64 {
	return uint64(time.Now().UnixMicro())
}

// encode serializes a packet as JSON or MessagePack. If the wire size
// exceeds MTUSafe, the payload is replaced with {"truncated": true} and
// re-encoded. Returns the bytes and whether truncation happened.
func encode(p Packet, useMsgpack bool) ([]byte, bool, error) {
	data, err := encodeRaw(p, useMsgpack)
	if err != nil {
		return nil, false, err
	}
	if len(data) <= MTUSafe {
		return data, false, nil
	}
	p.Payload = map[string]any{"truncated": true}
	data, err = encodeRaw(p, useMsgpack)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func encodeRaw(p Packet, useMsgpack bool) ([]byte, error) {
	if useMsgpack {
		return msgpack.Marshal(p)
	}
	return json.Marshal(p)
}
