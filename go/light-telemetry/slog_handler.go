package telemetry

import (
	"context"
	"log/slog"
	"strings"
)

// SlogHandler is a slog.Handler that emits each record as a telemetry
// packet under a fixed topic (default "logger").
//
// Payload = {level, message} + record/grouped attrs. Group attrs nest as
// maps. Handler methods never fail — the underlying client is
// fire-and-forget.
type SlogHandler struct {
	client *Client
	topic  string
	level  slog.Leveler
	attrs  []slog.Attr
	groups []string
}

// NewSlogHandler returns a handler emitting on client under the "logger"
// topic, at all levels.
func NewSlogHandler(client *Client) *SlogHandler {
	return &SlogHandler{client: client, topic: "logger", level: slog.LevelDebug}
}

// WithTopic returns a copy emitting under a different topic.
func (h *SlogHandler) WithTopic(topic string) *SlogHandler {
	h2 := *h
	h2.topic = topic
	return &h2
}

// WithMinLevel returns a copy filtering records below level.
func (h *SlogHandler) WithMinLevel(level slog.Leveler) *SlogHandler {
	h2 := *h
	h2.level = level
	return &h2
}

// Enabled implements slog.Handler.
func (h *SlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// Handle implements slog.Handler. Never fails.
func (h *SlogHandler) Handle(_ context.Context, r slog.Record) error {
	payload := map[string]any{
		"level":   r.Level.String(),
		"message": r.Message,
	}
	// Apply handler-level attrs (WithAttrs) first, then record attrs —
	// the record wins on key collisions, matching slog semantics.
	insert := insertInto(payload, h.groups)
	for _, a := range h.attrs {
		insert[a.Key] = attrValue(a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		insert[a.Key] = attrValue(a.Value)
		return true
	})
	h.client.Emit(h.topic, payload)
	return nil
}

// WithAttrs implements slog.Handler.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &h2
}

// WithGroup implements slog.Handler.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.groups = append(append([]string{}, h.groups...), name)
	return &h2
}

// insertInto walks/creates the nested map for the group path and returns
// the leaf map where attrs are written.
func insertInto(root map[string]any, groups []string) map[string]any {
	m := root
	for _, g := range groups {
		next, ok := m[g].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[g] = next
		}
		m = next
	}
	return m
}

// attrValue converts a slog.Value into a plain Go value for JSON encoding.
// Groups recurse into nested maps; LogValuer resolves; everything else
// falls back to its string form.
func attrValue(v slog.Value) any {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindGroup:
		m := map[string]any{}
		for _, a := range v.Group() {
			m[a.Key] = attrValue(a.Value)
		}
		return m
	case slog.KindString:
		return v.String()
	case slog.KindBool:
		return v.Bool()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time()
	case slog.KindAny:
		return v.Any()
	default:
		return strings.TrimSpace(v.String())
	}
}
