package tui

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"qattidev/sgsp"
	"qattidev/sgsp/examples/pong/internal/game"
	"qattidev/sgsp/examples/pong/internal/protocol"
)

func update(m Model, msg tea.Msg) Model {
	next, _ := m.Update(msg)
	return next.(Model)
}

func readyModel() Model {
	m := newModel(context.Background(), nil)
	m = update(m, tea.WindowSizeMsg{Width: 80, Height: 30})
	m.joined, m.connectionState, m.side = true, sgsp.Active, protocol.Left
	m.state = game.New().State
	m.state.Occupied, m.state.Connected = [2]bool{true, true}, [2]bool{true, true}
	m.target = m.state.Paddles[0]
	return m
}

func TestControlsOnlySetBoundedTargets(t *testing.T) {
	m := readyModel()
	before := m.state
	m = update(m, tea.KeyPressMsg{Code: 'w', Text: "w"})
	if m.target != before.Paddles[0]-1 || m.state != before {
		t.Fatal("input should change target without predicting server state")
	}
	m = update(m, tea.KeyPressMsg{Code: tea.KeyDown})
	if m.target != before.Paddles[0] {
		t.Fatal("down arrow did not move target")
	}
	for range 100 {
		m = update(m, tea.KeyPressMsg{Code: 's', Text: "s"})
	}
	if m.target != protocol.Height-protocol.PaddleHeight {
		t.Fatal("target escaped lower court boundary")
	}
	for range 100 {
		m = update(m, tea.KeyPressMsg{Code: tea.KeyUp})
	}
	if m.target != 0 {
		t.Fatal("target escaped upper court boundary")
	}
	m.connectionState = sgsp.Suspended
	m = update(m, tea.KeyPressMsg{Code: 's', Text: "s"})
	if m.target != 0 {
		t.Fatal("reconnecting client accepted input")
	}
}

func TestViewsAndResize(t *testing.T) {
	m := newModel(context.Background(), nil)
	m = update(m, tea.WindowSizeMsg{Width: 80, Height: 30})
	if !strings.Contains(m.View().Content, "Connecting") {
		t.Fatal("missing connecting state")
	}
	m = readyModel()
	m.state.Connected[1], m.state.Occupied[1] = false, false
	if !strings.Contains(m.View().Content, "Waiting for the other player") {
		t.Fatal("missing waiting state")
	}
	m.state.Occupied[1] = true
	if !strings.Contains(m.View().Content, "Opponent disconnected") {
		t.Fatal("missing opponent disconnect state")
	}
	m.connectionState = sgsp.Suspended
	if !strings.Contains(m.View().Content, "Reconnecting") {
		t.Fatal("missing reconnect state")
	}
	m = readyModel()
	m.state.Scores = [2]int{2, 3}
	view := m.View()
	if !view.AltScreen || !strings.Contains(view.Content, "You are left") || !strings.Contains(view.Content, "Left 2    :    3 Right") {
		t.Fatal("missing side, score, or alternate screen")
	}
	if strings.Count(court(m.state, true), "#") != 2*protocol.PaddleHeight {
		t.Fatal("court did not render both paddles")
	}
	if len(strings.Split(view.Content, "\n")) > minHeight {
		t.Fatal("view does not fit minimum terminal height")
	}
	m = update(m, tea.WindowSizeMsg{Width: 40, Height: 10})
	if !strings.Contains(m.View().Content, "resize terminal") {
		t.Fatal("missing resize prompt")
	}
	target := m.target
	m = update(m, tea.KeyPressMsg{Code: 's', Text: "s"})
	if m.target != target {
		t.Fatal("small terminal accepted invisible gameplay input")
	}
	m = update(m, tea.WindowSizeMsg{Width: 80, Height: 30})
	if strings.Contains(m.View().Content, "resize terminal") {
		t.Fatal("resize prompt remained after enlarging terminal")
	}
}

func TestSnapshotsErrorsAndQuit(t *testing.T) {
	m := readyModel()
	state := m.state
	state.Tick, state.Scores = 20, [2]int{1, 2}
	m = update(m, snapshotMsg(state))
	stale := state
	stale.Tick, stale.Scores = 19, [2]int{}
	m = update(m, snapshotMsg(stale))
	if m.state != state {
		t.Fatal("older snapshot replaced newer state")
	}
	failure := errors.New("game full")
	failed, cmd := m.Update(connectedMsg{err: failure})
	if failed.(Model).err != failure || cmd == nil {
		t.Fatal("connection failure did not exit with an error")
	}
	for _, msg := range []tea.KeyPressMsg{{Code: 'q', Text: "q"}, {Code: 'c', Mod: tea.ModCtrl}} {
		final, quit := m.Update(msg)
		if !final.(Model).quitting || quit == nil {
			t.Fatal("quit binding did not quit")
		}
		if _, ok := quit().(tea.QuitMsg); !ok {
			t.Fatal("quit command did not return QuitMsg")
		}
	}
}

func TestTransientSendFailureAllowsReconnect(t *testing.T) {
	m := readyModel()
	m.sending = true
	m = update(m, sentMsg{err: errors.New("QUIC connection timed out")})
	if m.err != nil || m.sending {
		t.Fatal("transient send failure prevented retry/reconnect")
	}
	failed, cmd := m.Update(sentMsg{err: sgsp.ErrInvalidArgument})
	if failed.(Model).err == nil || cmd == nil {
		t.Fatal("malformed application input was not reported")
	}
}
