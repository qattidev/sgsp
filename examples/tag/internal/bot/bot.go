// Package bot chooses client input; the server never runs autonomous players.
package bot

import (
	"math"
	"math/rand/v2"

	"qattidev/sgsp/examples/tag/internal/protocol"
)

// Controller retains a runner destination across input updates.
// Each client owns its own controller; its zero value is ready to use.
type Controller struct {
	target    protocol.Point
	hasTarget bool
	scores    [2]int
}

func (c *Controller) Input(s protocol.State, side protocol.Side) protocol.Input {
	p := s.Positions[side]
	target := s.Positions[1-side]
	if side == s.Chaser {
		c.hasTarget = false
	} else {
		if !c.hasTarget || c.scores != s.Scores || math.Hypot(c.target.X-p.X, c.target.Y-p.Y) <= 1 {
			// Choose uniformly within the three-cell wall margin, excluding
			// destinations closer than three cells to the runner.
			for {
				c.target = protocol.Point{
					X: 3 + rand.Float64()*float64(protocol.Width-7),
					Y: 3 + rand.Float64()*float64(protocol.Height-7),
				}
				if math.Hypot(c.target.X-p.X, c.target.Y-p.Y) >= 3 {
					break
				}
			}
			c.hasTarget = true
		}
		target = c.target
	}
	c.scores = s.Scores
	x, y := target.X-p.X, target.Y-p.Y
	if n := math.Hypot(x, y); n > 0 {
		x /= n
		y /= n
	}
	return protocol.Input{X: x, Y: y, Auto: true}
}
