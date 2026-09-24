# SGSP examples

| Example | What it demonstrates | Run from |
| --- | --- | --- |
| [Terminal Tag](tag/README.md) | Two-player terminal tag with client-side auto play, 120 Hz simulation and snapshots, and live rate statistics | `examples/tag` |
| [Pong](pong/README.md) | A playable Go game: one authoritative server, two Bubble Tea clients, request/reply, sequenced input and state, and reconnects | `examples/pong` |
| [Action](action/README.md) | Scripted input, reliable commands, inventory requests, raw streams, and optional bootstrap placement | Repository root |
| [API](api/main.go) | Minimal declarations of typed messages and requests; no networking | Repository root |

Start with Pong to see clients and a server running together. It has its own Go
module to keep the TUI dependencies separate from the SGSP library. The local
`replace` directive uses this checkout of SGSP; no published module is needed.
