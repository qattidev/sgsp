// Package game implements the server-owned fixed-step tag simulation.
package game

import (
	"math"
	"qattidev/sgsp/examples/tag/internal/protocol"
)

type Game struct {
	State  protocol.State
	inputs [2]protocol.Input
}

func New() *Game { g := &Game{}; g.respawn(); return g }
func (g *Game) respawn() {
	g.State.Positions = [2]protocol.Point{{X: 15, Y: 5}, {X: 44, Y: 14}}
	if (g.State.Scores[0]+g.State.Scores[1])%2 == 1 {
		g.State.Positions[0], g.State.Positions[1] = g.State.Positions[1], g.State.Positions[0]
	}
}
func (g *Game) SetPlayer(side protocol.Side, occupied, connected bool) {
	g.State.Occupied[side], g.State.Connected[side] = occupied, occupied && connected
	if !connected {
		g.inputs[side] = protocol.Input{}
	}
	if !occupied {
		g.State.Auto[side] = false
	}
	if !g.State.Occupied[0] && !g.State.Occupied[1] {
		tick, hz := g.State.Tick, g.State.Hz
		*g = *New()
		g.State.Tick, g.State.Hz = tick, hz
	}
}
func (g *Game) Input(side protocol.Side, in protocol.Input) {
	if math.IsNaN(in.X) || math.IsNaN(in.Y) || math.IsInf(in.X, 0) || math.IsInf(in.Y, 0) {
		return
	}
	n := math.Hypot(in.X, in.Y)
	if n > 1 {
		in.X /= n
		in.Y /= n
	}
	g.inputs[side] = in
	g.State.Auto[side] = in.Auto
}
func (g *Game) Step() {
	g.State.Tick++
	if !g.State.Playing() {
		return
	}
	g.State.Elapsed += 1.0 / protocol.TickRate
	for i, in := range g.inputs {
		speed := 16.0
		if protocol.Side(i) == g.State.Chaser {
			speed = 20
		} // Ensure frequent tags even in automatic play.
		p := &g.State.Positions[i]
		p.X = math.Max(0, math.Min(protocol.Width-1, p.X+in.X*speed/protocol.TickRate))
		p.Y = math.Max(0, math.Min(protocol.Height-1, p.Y+in.Y*speed/protocol.TickRate))
	}
	a, b := g.State.Positions[0], g.State.Positions[1]
	if math.Hypot(a.X-b.X, a.Y-b.Y) <= 1 {
		g.State.Scores[g.State.Chaser]++
		g.State.Chaser = 1 - g.State.Chaser
		g.respawn()
	}
}
