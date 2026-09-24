package tui

import (
	tea "charm.land/bubbletea/v2"
	"qattidev/sgsp/examples/tag/internal/protocol"
	"testing"
	"time"
)

func TestManualControls(t *testing.T) {
	m := Model{}
	for _, tc := range []struct {
		k    string
		x, y float64
	}{{"w", 0, -1}, {"s", 0, 1}, {"a", -1, 0}, {"d", 1, 0}} {
		model, _ := m.Update(tea.KeyPressMsg{Code: rune(tc.k[0]), Text: tc.k})
		m = model.(Model)
		if m.input.X != tc.x || m.input.Y != tc.y || time.Since(m.lastKey) > time.Second {
			t.Fatal(tc, m.input)
		}
		model, _ = m.Update(tea.KeyReleaseMsg{Code: rune(tc.k[0]), Text: tc.k})
		m = model.(Model)
		if m.input != (protocol.Input{}) {
			t.Fatal("release did not stop")
		}
	}
	for _, k := range []string{"up", "down", "left", "right"} {
		if _, _, ok := direction(k); !ok {
			t.Fatal(k)
		}
	}
	m.auto = true
	next, _ := m.Update(tea.KeyPressMsg{Code: 'w', Text: "w"})
	if next.(Model).input != (protocol.Input{}) {
		t.Fatal("manual key overrides auto")
	}
}
