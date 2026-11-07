// e2e-emit sends one JSON event then exits.
// Usage: e2e-emit <addr> <topic> <id> — driven by tests/e2e/run_e2e.py.
package main

import (
	"fmt"
	"os"

	telemetry "github.com/oldienki/telemetry/go/light-telemetry"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: e2e-emit <addr> <topic> <id>")
		os.Exit(2)
	}
	c, err := telemetry.NewClient(telemetry.Config{
		ServerAddr: os.Args[1],
		HostID:     os.Args[3],
		HostClass:  "e2e",
		// HeartbeatTimeout 0 → no watcher for a one-shot emitter.
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()
	c.Emit(os.Args[2], map[string]any{"lang": "go"})
	fmt.Println("emitted")
}
