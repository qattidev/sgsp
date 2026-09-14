package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	got, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestGoldenVectors(t *testing.T) {
	for value, want := range map[uint64]string{0: "00", 63: "3f", 64: "4040", 16383: "7fff", 16384: "80004000"} {
		got, err := AppendVarint(nil, value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, mustHex(t, want)) {
			t.Fatalf("V(%d) = %x, want %s", value, got, want)
		}
	}
	header, _ := EncodeReliableChannelHeaderFor(1)
	event, err := EncodeReliableEvent(Event{Channel: 1, MessageType: 10, Payload: []byte{0xaa, 0xbb}}, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if got := append(header, event...); !bytes.Equal(got, mustHex(t, "01010501010aaabb")) {
		t.Fatalf("reliable = %x", got)
	}
	decodedReliable, used, err := DecodeReliableEvent(event, 64<<10)
	if err != nil || used != len(event) || decodedReliable.Channel != 1 || decodedReliable.MessageType != 10 || !bytes.Equal(decodedReliable.Payload, []byte{0xaa, 0xbb}) {
		t.Fatalf("reliable decode = %#v, %d, %v", decodedReliable, used, err)
	}
	length, prefix, err := DecodeVarint(event)
	if err != nil {
		t.Fatal(err)
	}
	decodedHeader, payloadBytes, err := ReadReliableEventHeader(bytes.NewBuffer(event[prefix:]), length, 64<<10)
	if err != nil || decodedHeader.Channel != 1 || decodedHeader.MessageType != 10 || payloadBytes != 2 {
		t.Fatalf("reliable header = %#v, %d, %v", decodedHeader, payloadBytes, err)
	}
	for _, test := range []struct {
		event Event
		want  string
	}{
		{Event{Kind: UnreliableKind, Channel: 1, MessageType: 10, Payload: []byte{0xaa, 0xbb}}, "02010aaabb"},
		{Event{Kind: SequencedKind, Channel: 1, MessageType: 10, Sequence: 2, Payload: []byte{0xaa, 0xbb}}, "03010a02aabb"},
	} {
		got, err := EncodeDatagram(test.event, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, mustHex(t, test.want)) {
			t.Fatalf("datagram = %x, want %s", got, test.want)
		}
		decoded, err := DecodeDatagram(got, 1000)
		if err != nil || !bytes.Equal(decoded.Payload, test.event.Payload) {
			t.Fatalf("decode = %#v, %v", decoded, err)
		}
	}
	request, err := EncodeRequest(Request{MessageType: 20, TimeoutMS: 1000, Payload: []byte{0xaa}}, 64<<10)
	if err != nil || !bytes.Equal(request, mustHex(t, "021443e801aa")) {
		t.Fatalf("request = %x, %v", request, err)
	}
	if got, err := DecodeRequest(request, 64<<10); err != nil || got.MessageType != 20 || got.TimeoutMS != 1000 || !bytes.Equal(got.Payload, []byte{0xaa}) {
		t.Fatalf("decode request = %#v, %v", got, err)
	}
	response, _ := EncodeResponse(Response{Payload: []byte{0xbb}}, 64<<10)
	if !bytes.Equal(response, mustHex(t, "0001bb")) {
		t.Fatalf("response = %x", response)
	}
	unknown, _ := EncodeResponse(Response{Status: 13}, 64<<10)
	if !bytes.Equal(unknown, mustHex(t, "0d00")) {
		t.Fatalf("unknown = %x", unknown)
	}
	custom, _ := EncodeCustomStreamHeader(30)
	if !bytes.Equal(custom, mustHex(t, "031e")) {
		t.Fatalf("custom = %x", custom)
	}
	control, err := EncodeControl([]byte(`{"op":"close_ack"}`), PreNegotiationControlBytes)
	if err != nil || !bytes.Equal(control, mustHex(t, "127b226f70223a22636c6f73655f61636b227d")) {
		t.Fatalf("control = %x, %v", control, err)
	}
}

// TestGoldenVectorTestdata keeps the committed vectors usable by an external
// decoder rather than treating encode/decode round trips as their own oracle.
func TestGoldenVectorTestdata(t *testing.T) {
	raw, err := os.ReadFile("testdata/golden_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	var controlHex string
	if err := json.Unmarshal(vectors["control_close_ack"], &controlHex); err != nil {
		t.Fatal(err)
	}
	control, used, err := DecodeControl(mustHex(t, controlHex), PreNegotiationControlBytes)
	if err != nil || used != 19 || control.Op != "close_ack" {
		t.Fatalf("testdata control = %#v, %d, %v", control, used, err)
	}
	var requestHex string
	if err := json.Unmarshal(vectors["request_type_20_timeout_1000_aa"], &requestHex); err != nil {
		t.Fatal(err)
	}
	request, err := DecodeRequest(mustHex(t, requestHex), 64<<10)
	if err != nil || request.MessageType != 20 || request.TimeoutMS != 1000 || !bytes.Equal(request.Payload, []byte{0xaa}) {
		t.Fatalf("testdata request = %#v, %v", request, err)
	}
}

func TestFrameBoundaries(t *testing.T) {
	encoded, _ := EncodeControl([]byte(`{"op":"close_ack"}`), PreNegotiationControlBytes)
	for i := 0; i < len(encoded); i++ {
		if _, _, err := DecodeControl(encoded[:i], PreNegotiationControlBytes); !errors.Is(err, ErrTruncated) {
			t.Fatalf("truncation at %d = %v", i, err)
		}
	}
	if _, _, err := DecodeControl([]byte{0x80, 0x00, 0x40, 0x01}, PreNegotiationControlBytes); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize = %v", err)
	}
	if _, _, err := DecodeVarint([]byte{0x40, 0x01}); !errors.Is(err, ErrNonMinimal) {
		t.Fatalf("non-minimal = %v", err)
	}
}

func TestMalformedControl(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"op":"hello","op":"close"}`), []byte(`{"op":3}`), []byte(`{"op":"hello"}{}`), []byte{0xff}, []byte(`[]`)} {
		if _, err := DecodeControlBody(raw, PreNegotiationControlBytes); err == nil {
			t.Fatalf("accepted malformed control %q", raw)
		}
	}
	control, err := DecodeControlBody([]byte(`{"op":"hello","role":"game","app":"test","app_version":"1","required":["unknown"],"limits":{"control_bytes":1,"message_bytes":1,"datagram_bytes":1,"reliable_channels":1,"datagram_channels":1,"requests":1,"streams":1},"credential":{"scheme":"test","data":""}}`), PreNegotiationControlBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := RequireCapabilities(control, map[string]bool{"datagrams": true}); err == nil {
		t.Fatal("accepted unknown required capability")
	}
}

func TestDatagramValidation(t *testing.T) {
	for _, raw := range [][]byte{{0x02}, {0x03, 0x01, 0x0a}, {0x80, 0x00, 0x00, 0x02}} {
		if _, err := DecodeDatagram(raw, 1000); err == nil {
			t.Fatalf("accepted malformed datagram %x", raw)
		}
	}
	if _, err := DecodeDatagram(make([]byte, 1001), 1000); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize = %v", err)
	}
}

func TestStreamRoles(t *testing.T) {
	if _, _, err := DecodeReliableEvent([]byte{0x03, 0x02, 0x01, 0x01}, 64<<10); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("wrong reliable kind = %v", err)
	}
	if _, err := DecodeCustomStreamHeader([]byte{3, 1, 0}); err == nil {
		t.Fatal("accepted an extra custom stream header byte")
	}
	if _, err := DecodeRequest([]byte{3, 1, 1, 0}, 64<<10); !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("wrong request stream kind = %v", err)
	}
}

func FuzzControl(f *testing.F) {
	f.Add([]byte(`{"op":"hello"}`))
	f.Fuzz(func(t *testing.T, raw []byte) { _, _ = DecodeControlBody(raw, PreNegotiationControlBytes) })
}
func FuzzEvent(f *testing.F) {
	f.Add([]byte{2, 1, 1})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = DecodeDatagram(raw, 1000)
		_, _, _ = DecodeReliableEvent(raw, 64<<10)
	})
}
func FuzzRequest(f *testing.F) {
	f.Add([]byte{2, 1, 1, 0})
	f.Fuzz(func(t *testing.T, raw []byte) { _, _ = DecodeRequest(raw, 64<<10); _, _ = DecodeResponse(raw, 64<<10) })
}
