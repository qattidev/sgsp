package game

import (
	"math"
	"qattidev/sgsp/examples/tag/internal/bot"
	"qattidev/sgsp/examples/tag/internal/protocol"
	"testing"
)

func TestRules(t *testing.T) {
	g := New()
	g.SetPlayer(0, true, true)
	g.SetPlayer(1, true, true)
	g.State.Positions = [2]protocol.Point{{X: 4, Y: 4}, {X: 4.5, Y: 4}}
	g.Step()
	if g.State.Scores != [2]int{1, 0} || g.State.Chaser != 1 || g.State.Positions[0].X == 4 {
		t.Fatal(g.State)
	}
	g.Input(0, protocol.Input{X: math.NaN()})
	g.Input(0, protocol.Input{X: 100, Y: 100})
	old := g.State.Positions[0]
	g.Step()
	p := g.State.Positions[0]
	if math.Hypot(p.X-old.X, p.Y-old.Y) > 16.0/120+1e-9 {
		t.Fatal("speed not normalized")
	}
	for range 10000 {
		g.Step()
	}
	for _, p := range g.State.Positions {
		if p.X < 0 || p.X >= protocol.Width || p.Y < 0 || p.Y >= protocol.Height {
			t.Fatal("bounds")
		}
	}
	g.SetPlayer(1, true, false)
	oldState := g.State
	g.Step()
	if g.State.Positions != oldState.Positions || g.State.Elapsed != oldState.Elapsed {
		t.Fatal("pause")
	}
}
func TestAutomaticMotion(t *testing.T) {
	g := New()
	g.SetPlayer(0, true, true)
	g.SetPlayer(1, true, true)
	moved := [2]int{}
	controllers := [2]bot.Controller{}
	for range 120 * 120 {
		old := g.State
		for i := range 2 {
			g.Input(protocol.Side(i), controllers[i].Input(g.State, protocol.Side(i)))
		}
		g.Step()
		for i := range 2 {
			if old.Positions[i] != g.State.Positions[i] {
				moved[i]++
			}
		}
	}
	if g.State.Scores[0] < 5 || g.State.Scores[1] < 5 {
		t.Fatal(g.State.Scores)
	}
	for _, n := range moved {
		if n < 120*120*95/100 {
			t.Fatalf("too little movement: %v", moved)
		}
	}
}
