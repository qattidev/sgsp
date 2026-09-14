// The api example exists at M0 to keep the public typed helper signatures
// compiling while endpoint behavior is implemented in later milestones.
package main

import "qattidev/sgsp"

type input struct {
	Tick uint64 `json:"tick"`
}

type inventory struct {
	Items []string `json:"items"`
}

var inputs = sgsp.Message[input]{ID: 10, Codec: sgsp.JSON[input]()}
var inventoryQuery = sgsp.Request[struct{}, inventory]{
	ID:     11,
	Input:  sgsp.JSON[struct{}](),
	Output: sgsp.JSON[inventory](),
}

func main() {
	_, _ = inputs, inventoryQuery
}
