package wire

import (
	"errors"
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
