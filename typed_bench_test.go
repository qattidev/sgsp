package sgsp

import (
	"bytes"
	"testing"
)

type codecBenchmarkValue struct {
	Tick     uint64 `json:"tick"`
	Sequence uint64 `json:"sequence"`
	Payload  string `json:"payload"`
}

var codecBenchmarkSink any

// BenchmarkCodecRawBytes measures the application-side work of using a
// pre-encoded raw payload. It intentionally excludes SGSP framing and
// transport so JSON codec cost is not attributed to either transport trial.
func BenchmarkCodecRawBytes(b *testing.B) {
	payload := bytes.Repeat([]byte{'x'}, 512)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		codecBenchmarkSink = payload
	}
}

// BenchmarkCodecJSON measures the supplied public JSON codec's complete
// encode/decode path for a payload in the benchmark profile's size range.
func BenchmarkCodecJSON(b *testing.B) {
	codec := JSON[codecBenchmarkValue]()
	value := codecBenchmarkValue{Tick: 128, Sequence: 99, Payload: string(bytes.Repeat([]byte{'x'}, 480))}
	b.SetBytes(512)
	b.ReportAllocs()
	for b.Loop() {
		encoded, err := codec.Encode(value)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := codec.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
		codecBenchmarkSink = decoded
	}
}
