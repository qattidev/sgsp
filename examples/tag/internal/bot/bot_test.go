package bot

import (
	"math"
	"qattidev/sgsp/examples/tag/internal/protocol"
	"testing"
)

func TestRunnerKeepsDestinationAndChoosesAnotherOnArrival(t *testing.T) {
	c := Controller{}
	s := protocol.State{Chaser: 0, Positions: [2]protocol.Point{{X: 10, Y: 5}, {X: 55, Y: 18}}}
	in := c.Input(s, 1)
	target := c.target
	if target.X < 3 || target.X > protocol.Width-4 || target.Y < 3 || target.Y > protocol.Height-4 {
		t.Fatalf("runner should leave corner for interior: %+v, %+v", in, target)
	}
	s.Positions[0] = protocol.Point{X: 50, Y: 17}
	c.Input(s, 1)
	if c.target != target {
		t.Fatal("destination changed while travelling")
	}
	s.Positions[1] = target
	c.Input(s, 1)
	if math.Hypot(c.target.X-target.X, c.target.Y-target.Y) < 3 {
		t.Fatal("arrival did not select a destination at least three cells away")
	}
}

func TestChaserPursuesAndRoleSwapResetsDestination(t *testing.T) {
	c := Controller{hasTarget: true, target: protocol.Point{X: 7, Y: 7}}
	s := protocol.State{Chaser: 0, Positions: [2]protocol.Point{{X: 10, Y: 10}, {X: 13, Y: 14}}}
	in := c.Input(s, 0)
	if math.Abs(in.X-0.6) > 1e-9 || math.Abs(in.Y-0.8) > 1e-9 || !in.Auto || c.hasTarget {
		t.Fatalf("chase input: %+v", in)
	}
	s.Chaser = 1
	c.Input(s, 0)
	if !c.hasTarget {
		t.Fatal("new runner has no destination")
	}
	c.target = protocol.Point{X: 10, Y: 10}
	s.Scores[0]++
	s.Positions[0] = protocol.Point{X: 50, Y: 15}
	c.Input(s, 0)
	if c.target == (protocol.Point{X: 10, Y: 10}) {
		t.Fatal("respawn kept obsolete destination")
	}
}

// Sample repeatedly from a fixed runner position: the old opposite-half
// policy cannot cover all quadrants, while uniform selection should.
func TestRunnerDestinationsCoverWholeArena(t *testing.T) {
	c := Controller{}
	s := protocol.State{Chaser: 0, Positions: [2]protocol.Point{{X: 10, Y: 5}, {X: 55, Y: 18}}}
	var quadrants [4]int
	for range 10000 {
		c.hasTarget = false
		c.Input(s, 1)
		p := c.target
		if p.X < 3 || p.X > protocol.Width-4 || p.Y < 3 || p.Y > protocol.Height-4 {
			t.Fatal("destination outside interior", p)
		}
		quadrant := 0
		if p.X > float64(protocol.Width-1)/2 {
			quadrant++
		}
		if p.Y > float64(protocol.Height-1)/2 {
			quadrant += 2
		}
		quadrants[quadrant]++
	}
	for _, count := range quadrants {
		if count < 2000 || count > 3000 {
			t.Fatalf("biased destination distribution: %v", quadrants)
		}
	}
}

func TestRepeatedArrivalsChooseDistantDestinations(t *testing.T) {
	c := Controller{}
	s := protocol.State{Chaser: 0, Positions: [2]protocol.Point{{X: 10, Y: 5}, {X: 30, Y: 10}}}
	for range 1000 {
		c.Input(s, 1)
		p := s.Positions[1]
		if math.Hypot(c.target.X-p.X, c.target.Y-p.Y) < 3 {
			t.Fatal("destination too close")
		}
		if c.target.X < 3 || c.target.X > protocol.Width-4 || c.target.Y < 3 || c.target.Y > protocol.Height-4 {
			t.Fatal("wall margin")
		}
		s.Positions[1] = c.target
	}
}
