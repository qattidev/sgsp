// Package netclient is the client half of Pong's SGSP protocol, independent of
// Bubble Tea so the same connection can be exercised by integration tests.
package netclient

import (
	"context"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/examples/pong/internal/dev"
	"qattidev/sgsp/examples/pong/internal/protocol"
)

type Client struct {
	Connection sgsp.Client
	Updates    <-chan protocol.State
}

func Dial(ctx context.Context, address string, material dev.Material) (*Client, protocol.JoinReply, error) {
	credentials, err := material.Credentials()
	if err != nil {
		return nil, protocol.JoinReply{}, err
	}
	updates := make(chan protocol.State, 1)
	router := sgsp.NewRouter()
	if err := router.OnEvent(protocol.Snapshots.ID, func(_ context.Context, in *sgsp.Incoming) {
		if in.Epoch != in.Session.Epoch() || in.Channel != protocol.SnapshotOptions.Channel || in.Delivery != protocol.SnapshotOptions.Delivery {
			return
		}
		state, err := protocol.Snapshots.Codec.Decode(in.Payload)
		if err != nil {
			return
		}
		// A slow renderer needs the newest snapshot, not a backlog of frames.
		select {
		case updates <- state:
			return
		default:
		}
		select {
		case <-updates:
		default:
		}
		select {
		case updates <- state:
		default:
		}
	}); err != nil {
		return nil, protocol.JoinReply{}, err
	}
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connection, err := sgsp.Dial(connectCtx, sgsp.Endpoint{Address: address, ServerName: "localhost"}, sgsp.ClientConfig{
		TLS: material.ClientTLS(), App: protocol.App, Credentials: credentials,
		Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: router},
	})
	if err != nil {
		return nil, protocol.JoinReply{}, err
	}
	c := &Client{Connection: connection, Updates: updates}
	reply, err := sgsp.Call(connectCtx, connection.Session(), protocol.Join, struct{}{})
	if err != nil {
		c.Close()
		return nil, protocol.JoinReply{}, err
	}
	return c, reply, nil
}

func (c *Client) Send(ctx context.Context, target float64) error {
	return sgsp.Emit(ctx, c.Connection.Session(), protocol.Inputs, protocol.Input{Target: target}, protocol.InputOptions)
}

func (c *Client) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Connection.Close(ctx)
}
