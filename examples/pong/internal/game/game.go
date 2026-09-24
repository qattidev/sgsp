// Package game is a deterministic simulation with no networking or terminal I/O.
// Only the server advances it; clients display its snapshots.
package game

import (
	"math"

	"qattidev/sgsp/examples/pong/internal/protocol"
)

const (
	paddleSpeed = 24.0 // cells per second
	ballSpeed   = 22.0
	leftX       = 1.0
	rightX      = float64(protocol.Width - 2)
)

type Game struct {
	State          protocol.State
	targets        [2]float64
	velocity       protocol.Point
	serveTicks     int
	serveDirection float64
}

func New() *Game {
	g := &Game{serveDirection: 1}
	for i := range g.State.Paddles {
		g.State.Paddles[i] = float64(protocol.Height-protocol.PaddleHeight) / 2
		g.targets[i] = g.State.Paddles[i]
	}
	g.serve()
	return g
}

func (g *Game) SetPlayer(side protocol.Side, occupied, connected bool) {
	g.State.Occupied[side], g.State.Connected[side] = occupied, occupied && connected
	if !occupied {
		g.State.Paddles[side] = float64(protocol.Height-protocol.PaddleHeight) / 2
		g.targets[side] = g.State.Paddles[side]
	}
	if !g.State.Occupied[0] && !g.State.Occupied[1] {
		// Keep the snapshot counter monotonic across empty-room resets.
		tick := g.State.Tick
		*g = *New()
		g.State.Tick = tick
	}
}

func (g *Game) Input(side protocol.Side, target float64) {
	if math.IsNaN(target) || math.IsInf(target, 0) {
		return
	}
	g.targets[side] = math.Max(0, math.Min(float64(protocol.Height-protocol.PaddleHeight), target))
}

// Step advances exactly 1/60 second; pausing also freezes the serve countdown.
func (g *Game) Step() {
	g.State.Tick++
	if !g.State.Playing() {
		return
	}
	for i := range g.State.Paddles {
		delta := g.targets[i] - g.State.Paddles[i]
		g.State.Paddles[i] += math.Max(-paddleSpeed/protocol.TickRate, math.Min(paddleSpeed/protocol.TickRate, delta))
	}
	if g.serveTicks > 0 {
		g.serveTicks--
		g.State.Serving = g.serveTicks > 0
		return
	}
	oldX := g.State.Ball.X
	g.State.Ball.X += g.velocity.X / protocol.TickRate
	g.State.Ball.Y += g.velocity.Y / protocol.TickRate
	if g.State.Ball.Y < 0 {
		g.State.Ball.Y = -g.State.Ball.Y
		g.velocity.Y = math.Abs(g.velocity.Y)
	} else if g.State.Ball.Y > protocol.Height-1 {
		g.State.Ball.Y = 2*(protocol.Height-1) - g.State.Ball.Y
		g.velocity.Y = -math.Abs(g.velocity.Y)
	}
	if g.velocity.X < 0 && oldX > leftX && g.State.Ball.X <= leftX && g.hits(protocol.Left) {
		g.State.Ball.X = 2*leftX - g.State.Ball.X
		g.bounce(protocol.Left)
	} else if g.velocity.X > 0 && oldX < rightX && g.State.Ball.X >= rightX && g.hits(protocol.Right) {
		g.State.Ball.X = 2*rightX - g.State.Ball.X
		g.bounce(protocol.Right)
	}
	if g.State.Ball.X < 0 {
		g.State.Scores[protocol.Right]++
		g.serveDirection = -1
		g.serve()
	} else if g.State.Ball.X > protocol.Width-1 {
		g.State.Scores[protocol.Left]++
		g.serveDirection = 1
		g.serve()
	}
}

func (g *Game) hits(side protocol.Side) bool {
	return g.State.Ball.Y >= g.State.Paddles[side]-0.5 && g.State.Ball.Y <= g.State.Paddles[side]+protocol.PaddleHeight-0.5
}

func (g *Game) bounce(side protocol.Side) {
	center := g.State.Paddles[side] + float64(protocol.PaddleHeight-1)/2
	offset := (g.State.Ball.Y - center) / (float64(protocol.PaddleHeight) / 2)
	g.velocity.X = ballSpeed
	if side == protocol.Right {
		g.velocity.X = -ballSpeed
	}
	g.velocity.Y = offset * 16
	// Even a central hit eventually reaches a wall.
	if math.Abs(g.velocity.Y) < 3 {
		g.velocity.Y = 3
	}
}

func (g *Game) serve() {
	g.State.Ball = protocol.Point{X: float64(protocol.Width-1) / 2, Y: float64(protocol.Height-1) / 2}
	g.velocity = protocol.Point{X: g.serveDirection * ballSpeed, Y: 8}
	g.serveTicks = protocol.TickRate
	g.State.Serving = true
}
