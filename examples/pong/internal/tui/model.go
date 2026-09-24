// Package tui renders authoritative snapshots; it never simulates the ball or
// predicts paddle positions. Network work returns messages to Bubble Tea.
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
	"qattidev/sgsp/examples/pong/internal/dev"
	"qattidev/sgsp/examples/pong/internal/netclient"
	"qattidev/sgsp/examples/pong/internal/protocol"
)

const minWidth, minHeight = 64, 28

type bindings struct{ up, down, quit key.Binding }

func (k bindings) ShortHelp() []key.Binding  { return []key.Binding{k.up, k.down, k.quit} }
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
	target          float64
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
func Run(ctx context.Context, address string, material dev.Material) error {
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
	program := tea.NewProgram(newModel(ctx, connection), tea.WithContext(ctx))
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
		case m.joined && m.connectionState == sgsp.Active && m.width >= minWidth && m.height >= minHeight:
			if key.Matches(msg, controls.up) {
				m.target--
			}
			if key.Matches(msg, controls.down) {
				m.target++
			}
			m.target = math.Max(0, math.Min(protocol.Height-protocol.PaddleHeight, m.target))
		}
	case connectedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.client, m.side, m.state = msg.client, msg.reply.Side, msg.reply.State
		m.target, m.joined, m.connectionState = m.state.Paddles[m.side], true, sgsp.Active
		return m, m.awaitSnapshot()
	case snapshotMsg:
		if msg.Tick >= m.state.Tick {
			m.state = protocol.State(msg)
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
				return m, tea.Batch(nextFrame(), func() tea.Msg { return sentMsg{err: m.client.Send(m.ctx, m.target)} })
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
	if m.state.Serving {
		return "Get ready…"
	}
	return "Playing"
}

func (m Model) View() tea.View {
	var content string
	switch {
	case m.quitting:
		content = "Thanks for playing Pong!\n"
	case m.err != nil:
		content = "Pong: " + m.err.Error() + "\n"
	case m.width < minWidth || m.height < minHeight:
		content = fmt.Sprintf("Pong — resize terminal to at least %d×%d (currently %d×%d)\n\n%s", minWidth, minHeight, m.width, m.height, m.help.View(controls))
	default:
		title := "SGSP Pong"
		if m.joined {
			title += "  ·  You are " + m.side.String()
		}
		heading := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("86")).Render(title)
		score := fmt.Sprintf("Left %d    :    %d Right", m.state.Scores[0], m.state.Scores[1])
		content = heading + "\n" + score + "\n" + court(m.state, m.joined) + "\n" + m.status() + "\n" + m.help.View(controls)
	}
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

func court(state protocol.State, joined bool) string {
	var b strings.Builder
	b.WriteString("+" + strings.Repeat("-", protocol.Width) + "+\n")
	for y := 0; y < protocol.Height; y++ {
		b.WriteByte('|')
		for x := 0; x < protocol.Width; x++ {
			cell := byte(' ')
			if x == protocol.Width/2 && y%2 == 0 {
				cell = ':'
			}
			for side, px := range [2]int{1, protocol.Width - 2} {
				top := int(math.Round(state.Paddles[side]))
				if joined && state.Occupied[side] && x == px && y >= top && y < top+protocol.PaddleHeight {
					cell = '#'
				}
			}
			if joined && x == int(math.Round(state.Ball.X)) && y == int(math.Round(state.Ball.Y)) {
				cell = 'o'
			}
			b.WriteByte(cell)
		}
		b.WriteString("|\n")
	}
	b.WriteString("+" + strings.Repeat("-", protocol.Width) + "+")
	return b.String()
}
