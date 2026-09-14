package wire

import (
	"errors"
	"io"
)

const maxMessageType = uint64(1<<32 - 1)
const maxChannel = uint64(1<<16 - 1)

const (
	ReliableEventKind uint64 = 1
	UnreliableKind    uint64 = 2
	SequencedKind     uint64 = 3
	RequestStreamKind uint64 = 2
	CustomStreamKind  uint64 = 3
)

var (
	ErrInvalidKind = errors.New("sgsp wire: invalid frame kind")
	ErrTooLarge    = errors.New("sgsp wire: frame exceeds configured limit")
	ErrMalformed   = errors.New("sgsp wire: malformed frame")
)

type Event struct {
	Kind        uint64
	Channel     uint64
	MessageType uint64
	Sequence    uint64
	Payload     []byte
}

func appendEvent(dst []byte, event Event, sequence bool) ([]byte, error) {
	var err error
	for _, value := range []uint64{event.Kind, event.Channel, event.MessageType} {
		dst, err = AppendVarint(dst, value)
		if err != nil {
			return nil, err
		}
	}
	if sequence {
		dst, err = AppendVarint(dst, event.Sequence)
		if err != nil {
			return nil, err
		}
	}
	return append(dst, event.Payload...), nil
}

func EncodeReliableChannelHeader(channel uint64) ([]byte, error) {
	return EncodeReliableChannelHeaderFor(channel)
}

// EncodeReliableChannelHeaderFor returns a full event-channel stream prefix.
func EncodeReliableChannelHeaderFor(channel uint64) ([]byte, error) {
	b, err := AppendVarint(nil, ReliableEventKind)
	if err != nil {
		return nil, err
	}
	return AppendVarint(b, channel)
}

func EncodeReliableEvent(event Event, maxPayload int) ([]byte, error) {
	if event.Kind != 0 && event.Kind != ReliableEventKind {
		return nil, ErrInvalidKind
	}
	if maxPayload < 0 || len(event.Payload) > maxPayload {
		return nil, ErrTooLarge
	}
	event.Kind = ReliableEventKind
	if err := validateEvent(event, false); err != nil {
		return nil, err
	}
	body, err := appendEvent(nil, event, false)
	if err != nil {
		return nil, err
	}
	if len(body) > maxPayload+32 {
		return nil, ErrTooLarge
	}
	b, err := AppendVarint(nil, uint64(len(body)))
	if err != nil {
		return nil, err
	}
	return append(b, body...), nil
}

func DecodeReliableEvent(src []byte, maxPayload int) (Event, int, error) {
	length, n, err := DecodeVarint(src)
	if err != nil {
		return Event{}, 0, err
	}
	if length > uint64(maxPayload+32) || length > uint64(len(src)-n) {
		if length > uint64(maxPayload+32) {
			return Event{}, 0, ErrTooLarge
		}
		return Event{}, 0, ErrTruncated
	}
	body := src[n : n+int(length)]
	event, used, err := decodeEvent(body, ReliableEventKind, false)
	if err != nil {
		return Event{}, 0, err
	}
	if used > len(body) || len(event.Payload) > maxPayload {
		return Event{}, 0, ErrMalformed
	}
	return event, n + int(length), nil
}

// ReadReliableEventHeader validates and consumes the fixed event fields after
// a reliable frame's declared length. The caller can reserve exactly the
// returned payload length before allocating or reading that payload.
func ReadReliableEventHeader(reader io.ByteReader, length uint64, maxPayload int) (Event, int, error) {
	if reader == nil || maxPayload < 0 || length > uint64(maxPayload+32) {
		return Event{}, 0, ErrTooLarge
	}
	event := Event{Kind: ReliableEventKind}
	values := []*uint64{&event.Kind, &event.Channel, &event.MessageType}
	used := 0
	for _, value := range values {
		decoded, err := ReadVarint(reader)
		if err != nil {
			return Event{}, 0, err
		}
		*value = decoded
		used += VarintLen(decoded)
	}
	if event.Kind != ReliableEventKind {
		return Event{}, 0, ErrInvalidKind
	}
	if err := validateEvent(event, false); err != nil {
		return Event{}, 0, err
	}
	if uint64(used) > length {
		return Event{}, 0, ErrTruncated
	}
	payloadBytes := length - uint64(used)
	if payloadBytes > uint64(maxPayload) {
		return Event{}, 0, ErrTooLarge
	}
	return event, int(payloadBytes), nil
}

func EncodeDatagram(event Event, maxBytes int) ([]byte, error) {
	if event.Kind != UnreliableKind && event.Kind != SequencedKind {
		return nil, ErrInvalidKind
	}
	if maxBytes < 0 {
		return nil, ErrTooLarge
	}
	if err := validateEvent(event, event.Kind == SequencedKind); err != nil {
		return nil, err
	}
	b, err := appendEvent(nil, event, event.Kind == SequencedKind)
	if err != nil {
		return nil, err
	}
	if len(b) > maxBytes {
		return nil, ErrTooLarge
	}
	return b, nil
}

func DecodeDatagram(src []byte, maxBytes int) (Event, error) {
	if len(src) > maxBytes {
		return Event{}, ErrTooLarge
	}
	event, used, err := decodeEvent(src, 0, false)
	if err != nil {
		return Event{}, err
	}
	if event.Kind != UnreliableKind && event.Kind != SequencedKind {
		return Event{}, ErrInvalidKind
	}
	if event.Kind == SequencedKind {
		var n int
		event.Sequence, n, err = DecodeVarint(src[used:])
		if err != nil {
			return Event{}, err
		}
		used += n
	}
	if err := validateEvent(event, event.Kind == SequencedKind); err != nil {
		return Event{}, err
	}
	event.Payload = append([]byte(nil), src[used:]...)
	return event, nil
}

func validateEvent(event Event, sequenced bool) error {
	if event.Channel > maxChannel || event.MessageType == 0 || event.MessageType > maxMessageType {
		return ErrMalformed
	}
	if sequenced && event.Sequence == 0 {
		return ErrMalformed
	}
	return nil
}

func decodeEvent(src []byte, expectedKind uint64, sequence bool) (Event, int, error) {
	var event Event
	var n int
	var err error
	event.Kind, n, err = DecodeVarint(src)
	if err != nil {
		return event, 0, err
	}
	if expectedKind != 0 && event.Kind != expectedKind {
		return event, 0, ErrInvalidKind
	}
	used := n
	event.Channel, n, err = DecodeVarint(src[used:])
	if err != nil {
		return event, 0, err
	}
	used += n
	event.MessageType, n, err = DecodeVarint(src[used:])
	if err != nil {
		return event, 0, err
	}
	used += n
	if sequence {
		event.Sequence, n, err = DecodeVarint(src[used:])
		if err != nil {
			return event, 0, err
		}
		used += n
	}
	event.Payload = append([]byte(nil), src[used:]...)
	return event, used, nil
}
