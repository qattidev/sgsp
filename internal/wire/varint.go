// Package wire owns the pure SGSP framing and control codecs. It deliberately
// has no transport dependency, so malformed input can be rejected before any
// network or application state changes.
package wire

import (
	"errors"
	"io"
)

const MaxVarint = uint64(1<<62 - 1)

var (
	ErrTruncated     = errors.New("sgsp wire: truncated varint")
	ErrNonMinimal    = errors.New("sgsp wire: non-minimal varint")
	ErrValueTooLarge = errors.New("sgsp wire: value exceeds QUIC varint range")
)

func VarintLen(v uint64) int {
	switch {
	case v <= 63:
		return 1
	case v <= 16383:
		return 2
	case v <= 1073741823:
		return 4
	case v <= MaxVarint:
		return 8
	default:
		return 0
	}
}

func AppendVarint(dst []byte, v uint64) ([]byte, error) {
	n := VarintLen(v)
	if n == 0 {
		return nil, ErrValueTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, n)...)
	switch n {
	case 1:
		dst[start] = byte(v)
	case 2:
		dst[start] = byte(v>>8) | 0x40
		dst[start+1] = byte(v)
	case 4:
		dst[start] = byte(v>>24) | 0x80
		dst[start+1] = byte(v >> 16)
		dst[start+2] = byte(v >> 8)
		dst[start+3] = byte(v)
	case 8:
		dst[start] = byte(v>>56) | 0xc0
		for i := 1; i < 8; i++ {
			dst[start+i] = byte(v >> uint(8*(7-i)))
		}
	}
	return dst, nil
}

func DecodeVarint(src []byte) (uint64, int, error) {
	if len(src) == 0 {
		return 0, 0, ErrTruncated
	}
	n := 1 << (src[0] >> 6)
	if len(src) < n {
		return 0, 0, ErrTruncated
	}
	v := uint64(src[0] & 0x3f)
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(src[i])
	}
	if VarintLen(v) != n {
		return 0, 0, ErrNonMinimal
	}
	return v, n, nil
}

// ReadVarint reads exactly one minimal QUIC variable-length integer.
func ReadVarint(r io.ByteReader) (uint64, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	n := 1 << (first >> 6)
	v := uint64(first & 0x3f)
	for i := 1; i < n; i++ {
		b, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, ErrTruncated
			}
			return 0, err
		}
		v = v<<8 | uint64(b)
	}
	if VarintLen(v) != n {
		return 0, ErrNonMinimal
	}
	return v, nil
}
