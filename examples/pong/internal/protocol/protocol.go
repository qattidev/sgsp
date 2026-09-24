// Package protocol defines the entire application protocol for Pong.
// SGSP provides authentication, sessions, request/reply, and sequenced delivery.
package protocol

import "qattidev/sgsp"

const (
	AppID        = "sgsp-pong"
	Address      = "127.0.0.1:4444"
	DevDir       = ".sgsp-pong-dev"
	Width        = 60
	Height       = 20
	PaddleHeight = 4
	TickRate     = 60
	UpdateRate   = 30
)

var App = sgsp.AppIdentity{ID: AppID, Version: "1"}

type Side int

const (
	Left Side = iota
	Right
)

func (s Side) String() string {
	if s == Left {
		return "left"
	}
	return "right"
}

// Target is the desired top row of the paddle, not a client-computed position.
// Resending the latest target repairs packet loss without repeating a movement.
type Input struct {
	Target float64 `json:"target"`
}

type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// State is a complete snapshot, so missing a datagram needs no recovery request.
type State struct {
	Tick      uint64     `json:"tick"`
	Ball      Point      `json:"ball"`
	Paddles   [2]float64 `json:"paddles"`
	Scores    [2]int     `json:"scores"`
	Occupied  [2]bool    `json:"occupied"`
	Connected [2]bool    `json:"connected"`
	Serving   bool       `json:"serving"`
}

func (s State) Playing() bool { return s.Connected[0] && s.Connected[1] }

type JoinReply struct {
	Side  Side  `json:"side"`
	State State `json:"state"`
}

var (
	Join            = sgsp.Request[struct{}, JoinReply]{ID: 10, Input: sgsp.JSON[struct{}](), Output: sgsp.JSON[JoinReply]()}
	Inputs          = sgsp.Message[Input]{ID: 11, Codec: sgsp.JSON[Input]()}
	Snapshots       = sgsp.Message[State]{ID: 12, Codec: sgsp.JSON[State]()}
	InputOptions    = sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}
	SnapshotOptions = sgsp.SendOptions{Channel: 2, Delivery: sgsp.UnreliableSequenced}
)
