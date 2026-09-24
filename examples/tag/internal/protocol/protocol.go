// Package protocol defines the entire application protocol for Tag.
// SGSP provides authentication, sessions, request/reply, and sequenced delivery.
package protocol

import "qattidev/sgsp"

const (
	AppID   = "sgsp-tag"
	Address = "127.0.0.1:4445"
	DevDir  = ".sgsp-tag-dev"
	Width   = 60
	Height  = 20

	TickRate   = 120
	UpdateRate = 120
)

var App = sgsp.AppIdentity{ID: AppID, Version: "1"}

type Side int

const (
	A Side = iota
	B
)

func (s Side) String() string {
	if s == A {
		return "A"
	}
	return "B"
}

// Input is a direction, applied at a server-controlled speed.
type Input struct {
	X    float64 `json:"x"`
	Y    float64 `json:"y"`
	Auto bool    `json:"auto"`
}

type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// State is a complete snapshot, so missing a datagram needs no recovery request.
type State struct {
	Tick      uint64   `json:"tick"`
	Positions [2]Point `json:"positions"`
	Chaser    Side     `json:"chaser"`
	Auto      [2]bool  `json:"auto"`
	Elapsed   float64  `json:"elapsed"`
	Hz        float64  `json:"hz"`
	Scores    [2]int   `json:"scores"`
	Occupied  [2]bool  `json:"occupied"`
	Connected [2]bool  `json:"connected"`
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
