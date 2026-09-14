package sgsp

import (
	"context"
	"encoding/json"
	"errors"
)

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}
type Message[T any] struct {
	ID    MessageType
	Codec Codec[T]
}
type Request[I, O any] struct {
	ID     MessageType
	Input  Codec[I]
	Output Codec[O]
}
type jsonCodec[T any] struct{}

func JSON[T any]() Codec[T]                     { return jsonCodec[T]{} }
func (jsonCodec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec[T]) Decode(b []byte) (T, error) { var v T; err := json.Unmarshal(b, &v); return v, err }
func Emit[T any](ctx context.Context, s Session, m Message[T], value T, opts SendOptions) error {
	b, err := m.Codec.Encode(value)
	if err != nil {
		return err
	}
	return s.Send(ctx, m.ID, b, opts)
}
func Call[I, O any](ctx context.Context, s Session, r Request[I, O], input I) (O, error) {
	var zero O
	b, err := r.Input.Encode(input)
	if err != nil {
		return zero, err
	}
	b, err = s.Call(ctx, r.ID, b)
	if err != nil {
		return zero, err
	}
	return r.Output.Decode(b)
}
func OnMessage[T any](router *Router, message Message[T], handler func(context.Context, Session, T) error) error {
	if message.Codec == nil || handler == nil {
		return ErrInvalidArgument
	}
	return router.OnEvent(message.ID, func(ctx context.Context, incoming *Incoming) {
		value, err := message.Codec.Decode(incoming.Payload)
		if err == nil {
			_ = handler(ctx, incoming.Session, value)
		}
	})
}
func OnCall[I, O any](router *Router, request Request[I, O], handler func(context.Context, Session, I) (O, error)) error {
	if request.Input == nil || request.Output == nil || handler == nil {
		return ErrInvalidArgument
	}
	return router.OnRequest(request.ID, func(ctx context.Context, incoming *Incoming) {
		value, err := request.Input.Decode(incoming.Payload)
		if err != nil {
			_ = incoming.Fail(ctx, InvalidArgument, "invalid request payload")
			return
		}
		result, err := handler(ctx, incoming.Session, value)
		if err != nil {
			var protocol *Error
			if errors.As(err, &protocol) && protocol.Code != Normal {
				_ = incoming.Fail(ctx, protocol.Code, protocol.Message)
			} else {
				_ = incoming.Fail(ctx, Internal, "request failed")
			}
			return
		}
		encoded, err := request.Output.Encode(result)
		if err != nil {
			_ = incoming.Fail(ctx, Internal, "response encoding failed")
			return
		}
		_ = incoming.Reply(ctx, encoded)
	})
}
