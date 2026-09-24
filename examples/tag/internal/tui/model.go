// Package tui displays authoritative tag snapshots and produces player input.
package tui

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"qattidev/sgsp"
	"qattidev/sgsp/examples/tag/internal/bot"
	"qattidev/sgsp/examples/tag/internal/dev"
	"qattidev/sgsp/examples/tag/internal/netclient"
	"qattidev/sgsp/examples/tag/internal/protocol"
)

const minWidth, minHeight = 90, 30

type bindings struct{ up, down, quit key.Binding }

func (k bindings) ShortHelp() []key.Binding {
	return []key.Binding{k.up, k.down, key.NewBinding(key.WithHelp("←/a →/d", "left/right")), k.quit}
}
func (k bindings) FullHelp() [][]key.Binding { return [][]key.Binding{k.ShortHelp()} }

var controls = bindings{
	up:   key.NewBinding(key.WithKeys("up", "w"), key.WithHelp("↑/w", "up")),
	down: key.NewBinding(key.WithKeys("down", "s"), key.WithHelp("↓/s", "down")),
	quit: key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q/ctrl+c", "quit")),
}

type connectedMsg struct {
	client *netclient.Client
	reply  protocol.JoinReply
	err    error
}
type snapshotMsg protocol.State
type frameMsg time.Time
type sentMsg struct{ err error }

type Model struct {
	ctx             context.Context
	connection      <-chan connectedMsg
	client          *netclient.Client
	state           protocol.State
	side            protocol.Side
	input           protocol.Input
	auto            bool
	bot             bot.Controller
	releases        bool
	lastKey         time.Time
	sampleAt        time.Time
	updates         int
	updateHz        float64
	joined          bool
	sending         bool
	connectionState sgsp.State
	width, height   int
	help            help.Model
	spinner         spinner.Model
	err             error
	quitting        bool
}

func newModel(ctx context.Context, connection <-chan connectedMsg) Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	return Model{ctx: ctx, connection: connection, help: help.New(), spinner: s, connectionState: sgsp.Connecting}
}

// Run owns the dial worker and its eventual connection, including when the
// user quits before dialing finishes or Bubble Tea cannot initialize a TTY.
func Run(ctx context.Context, address string, material dev.Material, auto bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	connection := make(chan connectedMsg, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		client, reply, err := netclient.Dial(ctx, address, material)
		connection <- connectedMsg{client: client, reply: reply, err: err}
		if client != nil {
			<-ctx.Done()
			client.Close()
		}
	}()
	model := newModel(ctx, connection)
	model.auto = auto
	model.sampleAt = time.Now()
	program := tea.NewProgram(model, tea.WithContext(ctx), tea.WithFPS(60))
	final, err := program.Run()
	wasCanceled := ctx.Err() != nil
	cancel()
	<-done
	if err != nil {
		if errors.Is(err, tea.ErrProgramKilled) && wasCanceled {
			return nil
		}
		return err
	}
	if model, ok := final.(Model); ok {
		return model.err
	}
	return nil
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, nextFrame(), func() tea.Msg {
		select {
		case value := <-m.connection:
			return value
		case <-m.ctx.Done():
			return nil
		}
	})
}

func nextFrame() tea.Cmd {
	return tea.Tick(time.Second/protocol.UpdateRate, func(now time.Time) tea.Msg { return frameMsg(now) })
}

func (m Model) awaitSnapshot() tea.Cmd {
	return func() tea.Msg {
		select {
		case state := <-m.client.Updates:
			return snapshotMsg(state)
		case <-m.ctx.Done():
			return nil
		}
	}
}

func (m Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.help.SetWidth(msg.Width)
	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, controls.quit):
			m.quitting = true
			return m, tea.Quit
		case !m.auto:
			if x, y, ok := direction(msg.String()); ok {
				m.input = protocol.Input{X: x, Y: y}
				m.lastKey = time.Now()
			}
		}
	case tea.KeyboardEnhancementsMsg:
		m.releases = msg.SupportsEventTypes()
	case tea.KeyReleaseMsg:
		if x, y, ok := direction(msg.String()); ok && m.input.X == x && m.input.Y == y {
			m.input = protocol.Input{}
		}

	case connectedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.client, m.side, m.state = msg.client, msg.reply.Side, msg.reply.State
		m.joined, m.connectionState = true, sgsp.Active
		return m, m.awaitSnapshot()
	case snapshotMsg:
		if msg.Tick >= m.state.Tick {
			m.state = protocol.State(msg)
			m.updates++
			if elapsed := time.Since(m.sampleAt).Seconds(); elapsed >= 1 {
				m.updateHz = float64(m.updates) / elapsed
				m.updates = 0
				m.sampleAt = time.Now()
			}
		}
		return m, m.awaitSnapshot()
	case frameMsg:
		if m.client != nil {
			m.connectionState = m.client.Connection.Session().State()
			if m.connectionState == sgsp.Closed {
				m.err = errors.New("session ended; restart the client to join again")
				return m, tea.Quit
			}
			if m.connectionState == sgsp.Active && !m.sending {
				m.sending = true
				if m.auto {
					m.input = m.bot.Input(m.state, m.side)
				} else if !m.releases && time.Since(m.lastKey) > 600*time.Millisecond {
					m.input = protocol.Input{}
				}
				input := m.input
				return m, tea.Batch(nextFrame(), func() tea.Msg { return sentMsg{err: m.client.Send(m.ctx, input)} })
			}
		}
		return m, nextFrame()
	case sentMsg:
		m.sending = false
		// A send on the previous QUIC connection can fail just before SGSP
		// reports Suspended. Retry replaceable input; session state decides
		// whether recovery is over. Only malformed application sends are fatal.
		if errors.Is(msg.err, sgsp.ErrInvalidArgument) || errors.Is(msg.err, sgsp.ErrTooLarge) || errors.Is(msg.err, sgsp.ErrProtocolViolation) {
			m.err = msg.err
			return m, tea.Quit
		}
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m Model) status() string {
	if !m.joined {
		return m.spinner.View() + " Connecting to server…"
	}
	if m.connectionState == sgsp.Suspended {
		return m.spinner.View() + " Reconnecting… game paused"
	}
	if !m.state.Playing() {
		other := 1 - int(m.side)
		if m.state.Occupied[other] {
			return m.spinner.View() + " Opponent disconnected — game paused"
		}
		return m.spinner.View() + " Waiting for the other player…"
	}
	return "Playing"
}

func (m Model) View() tea.View {
	var content string
	switch {
	case m.quitting:
		content = "Thanks for playing Tag!\n"
	case m.err != nil:
		content = "Tag: " + m.err.Error() + "\n"
	case m.width < minWidth || m.height < minHeight:
		content = fmt.Sprintf("Tag — resize terminal to at least %d×%d (currently %d×%d)\n\n%s", minWidth, minHeight, m.width, m.height, m.help.View(controls))
	default:
		title := "SGSP Tag"
		if m.joined {
			title += "  ·  You are " + m.side.String()
		}
		heading := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86")).Render(title)
		mode := func(v bool) string {
			if v {
				return "AUTO"
			}
			return "MANUAL"
		}
		score := fmt.Sprintf("A: %d   B: %d   Chaser: %s   Time: %.1fs\nSERVER: 120 Hz (measured %.1f)  Updates: %.1f/s  Render cap: 60 FPS\nMODE: %s vs %s   Tick: %d", m.state.Scores[0], m.state.Scores[1], m.state.Chaser, m.state.Elapsed, m.state.Hz, m.updateHz, mode(m.state.Auto[0]), mode(m.state.Auto[1]), m.state.Tick)
		content = heading + "\n" + score + "\n" + court(m.state, m.joined) + "\n" + m.status() + "\n" + m.help.View(controls)
	}
	v := tea.NewView(content)
	v.AltScreen = true
	v.KeyboardEnhancements.ReportEventTypes = true
	return v
}

func court(state protocol.State, joined bool) string {
	var b strings.Builder
	b.WriteString("+" + strings.Repeat("-", protocol.Width) + "+\n")
	for y := 0; y < protocol.Height; y++ {
		b.WriteByte('|')
		for x := 0; x < protocol.Width; x++ {
			cell := byte(' ')
			for i, p := range state.Positions {
				if joined && state.Occupied[i] && x == int(math.Round(p.X)) && y == int(math.Round(p.Y)) {
					cell = byte('A' + i)
				}
			}
			b.WriteByte(cell)
		}
		b.WriteString("|\n")
	}
	b.WriteString("+" + strings.Repeat("-", protocol.Width) + "+")
	return b.String()
}

func direction(key string) (float64, float64, bool) {
	switch key {
	case "w", "up":
		return 0, -1, true
	case "s", "down":
		return 0, 1, true
	case "a", "left":
		return -1, 0, true
	case "d", "right":
		return 1, 0, true
	}
	return 0, 0, false
}
