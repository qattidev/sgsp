package wire

import (
	"errors"
	"io"
	"unicode/utf8"
)

const maxDiagnosticBytes = 256

type Request struct {
	MessageType, TimeoutMS uint64
	Payload                []byte
}
type Response struct {
	Status  uint64
	Payload []byte
}

func EncodeRequest(request Request, maxPayload int) ([]byte, error) {
	if request.MessageType == 0 || request.MessageType > maxMessageType || request.TimeoutMS == 0 || request.TimeoutMS > 300000 || len(request.Payload) > maxPayload {
		return nil, ErrMalformed
	}
	b, err := AppendVarint(nil, RequestStreamKind)
	if err != nil {
		return nil, err
	}
	for _, v := range []uint64{request.MessageType, request.TimeoutMS, uint64(len(request.Payload))} {
		b, err = AppendVarint(b, v)
		if err != nil {
			return nil, err
		}
	}
	return append(b, request.Payload...), nil
}
func DecodeRequest(src []byte, maxPayload int) (Request, error) {
	var out Request
	values := make([]uint64, 4)
	used := 0
	for i := range values {
		value, n, err := DecodeVarint(src[used:])
		if err != nil {
			return out, err
		}
		values[i] = value
		used += n
	}
	if values[0] != RequestStreamKind || values[1] == 0 || values[1] > maxMessageType || values[3] > uint64(maxPayload) || values[3] != uint64(len(src)-used) || values[2] == 0 || values[2] > 300000 {
		return out, ErrMalformed
	}
	return Request{MessageType: values[1], TimeoutMS: values[2], Payload: append([]byte(nil), src[used:]...)}, nil
}

// ReadRequestHeader consumes the request fields after the stream-kind prefix
// and returns the declared payload length. Callers can reserve that exact
// payload before allocating or reading it from a reliable stream.
func ReadRequestHeader(reader io.ByteReader, maxPayload int) (Request, int, error) {
	if reader == nil || maxPayload < 0 {
		return Request{}, 0, ErrMalformed
	}
	messageType, err := ReadVarint(reader)
	if err != nil {
		return Request{}, 0, err
	}
	timeoutMS, err := ReadVarint(reader)
	if err != nil {
		return Request{}, 0, err
	}
	payloadBytes, err := ReadVarint(reader)
	if err != nil {
		return Request{}, 0, err
	}
	if messageType == 0 || messageType > maxMessageType || timeoutMS == 0 || timeoutMS > 300000 || payloadBytes > uint64(maxPayload) {
		return Request{}, 0, ErrMalformed
	}
	return Request{MessageType: messageType, TimeoutMS: timeoutMS}, int(payloadBytes), nil
}
func EncodeResponse(response Response, maxPayload int) ([]byte, error) {
	if len(response.Payload) > maxPayload {
		return nil, ErrTooLarge
	}
	if response.Status != 0 && (len(response.Payload) > maxDiagnosticBytes || !utf8.Valid(response.Payload)) {
		return nil, ErrMalformed
	}
	b, err := AppendVarint(nil, response.Status)
	if err != nil {
		return nil, err
	}
	b, err = AppendVarint(b, uint64(len(response.Payload)))
	if err != nil {
		return nil, err
	}
	return append(b, response.Payload...), nil
}
func DecodeResponse(src []byte, maxPayload int) (Response, error) {
	status, n, err := DecodeVarint(src)
	if err != nil {
		return Response{}, err
	}
	length, m, err := DecodeVarint(src[n:])
	if err != nil {
		return Response{}, err
	}
	if length > uint64(maxPayload) || length != uint64(len(src)-n-m) || (status != 0 && (length > maxDiagnosticBytes || !utf8.Valid(src[n+m:]))) {
		return Response{}, ErrMalformed
	}
	return Response{Status: status, Payload: append([]byte(nil), src[n+m:]...)}, nil
}

// ReadResponseHeader consumes a response status and declared body length
// without allocating that body. The caller must still validate diagnostic
// UTF-8 after it reads a non-success payload.
func ReadResponseHeader(reader io.ByteReader, maxPayload int) (Response, int, error) {
	if reader == nil || maxPayload < 0 {
		return Response{}, 0, ErrMalformed
	}
	status, err := ReadVarint(reader)
	if err != nil {
		return Response{}, 0, err
	}
	payloadBytes, err := ReadVarint(reader)
	if err != nil {
		return Response{}, 0, err
	}
	if payloadBytes > uint64(maxPayload) || (status != 0 && payloadBytes > maxDiagnosticBytes) {
		return Response{}, 0, ErrMalformed
	}
	return Response{Status: status}, int(payloadBytes), nil
}
func EncodeCustomStreamHeader(messageType uint64) ([]byte, error) {
	if messageType == 0 || messageType > maxMessageType {
		return nil, ErrMalformed
	}
	b, err := AppendVarint(nil, CustomStreamKind)
	if err != nil {
		return nil, err
	}
	return AppendVarint(b, messageType)
}
func DecodeCustomStreamHeader(src []byte) (uint64, error) {
	kind, n, err := DecodeVarint(src)
	if err != nil {
		return 0, err
	}
	if kind != CustomStreamKind {
		return 0, ErrInvalidKind
	}
	typ, m, err := DecodeVarint(src[n:])
	if err != nil || n+m != len(src) {
		if err == nil {
			err = errors.New("sgsp wire: extra custom stream header bytes")
		}
		return 0, err
	}
	if typ == 0 || typ > maxMessageType {
		return 0, ErrMalformed
	}
	return typ, nil
}
