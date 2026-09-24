package game

import (
	"math"
	"testing"

	"qattidev/sgsp/examples/pong/internal/protocol"
)

func playingGame() *Game {
	g := New()
	g.SetPlayer(protocol.Left, true, true)
	g.SetPlayer(protocol.Right, true, true)
	return g
}

func TestWaitingServeAndPause(t *testing.T) {
	g := New()
	initial := g.State.Ball
	for range protocol.TickRate {
		g.Step()
	}
	if g.State.Ball != initial || g.serveTicks != protocol.TickRate {
		t.Fatal("waiting advanced the ball or serve countdown")
	}
	g.SetPlayer(protocol.Left, true, true)
	g.SetPlayer(protocol.Right, true, true)
	for range protocol.TickRate {
		g.Step()
	}
	if g.State.Ball != initial || g.State.Serving {
		t.Fatal("serve should finish after one second without moving the ball early")
	}
	g.Step()
	if g.State.Ball == initial {
		t.Fatal("ball did not move after the serve")
	}
	g.SetPlayer(protocol.Right, true, false)
	paused := g.State
	for range 100 {
		g.Step()
	}
	if g.State.Ball != paused.Ball || g.State.Paddles != paused.Paddles || g.State.Scores != paused.Scores {
		t.Fatal("disconnected game was not frozen")
	}
	g.SetPlayer(protocol.Right, true, true)
	g.Step()
	if g.State.Ball == paused.Ball {
		t.Fatal("resumed game did not advance")
	}
}

func TestPaddleBoundsSpeedAndInvalidInput(t *testing.T) {
	g := playingGame()
	g.Input(protocol.Left, -100)
	g.Input(protocol.Right, 100)
	start := g.State.Paddles
	g.Step()
	for side := range start {
		if math.Abs(g.State.Paddles[side]-start[side]) > paddleSpeed/protocol.TickRate+1e-9 {
			t.Fatal("paddle teleported")
		}
	}
	for range 100 {
		g.Step()
	}
	if g.State.Paddles != [2]float64{0, protocol.Height - protocol.PaddleHeight} {
		t.Fatalf("paddle bounds: %v", g.State.Paddles)
	}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		g.Input(protocol.Left, invalid)
	}
	g.Step()
	if g.State.Paddles[0] != 0 {
		t.Fatal("invalid target changed paddle position")
	}
}

func TestCollisionsAndScoring(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		ball, velocity       protocol.Point
		score                [2]int
		wantXSign, wantYSign float64
	}{
		{name: "top wall", ball: protocol.Point{X: 30, Y: 0.01}, velocity: protocol.Point{X: 22, Y: -8}, wantXSign: 1, wantYSign: 1},
		{name: "bottom wall", ball: protocol.Point{X: 30, Y: 18.99}, velocity: protocol.Point{X: 22, Y: 8}, wantXSign: 1, wantYSign: -1},
		{name: "left paddle", ball: protocol.Point{X: 1.1, Y: 9}, velocity: protocol.Point{X: -22, Y: 3}, wantXSign: 1, wantYSign: -1},
		{name: "right paddle", ball: protocol.Point{X: 57.9, Y: 9}, velocity: protocol.Point{X: 22, Y: 3}, wantXSign: -1, wantYSign: -1},
		{name: "left miss", ball: protocol.Point{X: 0.1, Y: 1}, velocity: protocol.Point{X: -22, Y: 3}, score: [2]int{0, 1}},
		{name: "right miss", ball: protocol.Point{X: 58.9, Y: 1}, velocity: protocol.Point{X: 22, Y: 3}, score: [2]int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := playingGame()
			g.serveTicks, g.State.Serving = 0, false
			g.State.Ball, g.velocity = tc.ball, tc.velocity
			g.Step()
			if g.State.Scores != tc.score {
				t.Fatalf("scores: got %v, want %v", g.State.Scores, tc.score)
			}
			if tc.score != [2]int{} {
				if !g.State.Serving || g.State.Ball != New().State.Ball {
					t.Fatal("point did not reset ball and start serve")
				}
				g.Step()
				if g.State.Scores != tc.score {
					t.Fatal("point scored twice")
				}
			} else if g.velocity.X*tc.wantXSign <= 0 || g.velocity.Y*tc.wantYSign <= 0 {
				t.Fatalf("unexpected bounce velocity: %v", g.velocity)
			}
		})
	}
}

func TestRoomResetAndDeterminism(t *testing.T) {
	a, b := playingGame(), playingGame()
	for tick := range 1000 {
		for _, g := range []*Game{a, b} {
			g.Input(protocol.Left, float64(tick%17))
			g.Step()
		}
		if a.State != b.State {
			t.Fatal("same input produced different simulation states")
		}
	}
	a.State.Scores = [2]int{3, 4}
	a.SetPlayer(protocol.Left, false, false)
	if a.State.Scores != [2]int{3, 4} {
		t.Fatal("departing player reset occupied room's scores")
	}
	tick := a.State.Tick
	a.SetPlayer(protocol.Right, false, false)
	if a.State.Scores != [2]int{} || a.State.Tick != tick {
		t.Fatal("empty room did not reset scores while preserving snapshot order")
	}
}
