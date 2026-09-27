package objstore

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func BenchmarkReadBodyKnownLength(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i*37 + 11)
			}
			n := int64(size)
			b.SetBytes(n)
			b.ReportAllocs()
			for b.Loop() {
				got, err := readBody("get-range", "pack", io.NopCloser(bytes.NewReader(data)), &n)
				if err != nil || !bytes.Equal(got, data) {
					b.Fatalf("body mismatch: %v", err)
				}
			}
		})
	}
}
