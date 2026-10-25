package record_test

import (
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/record"
)

var retainedClone record.Bytes

func BenchmarkBytesClone(b *testing.B) {
	for _, sizes := range [][2]int{{1, 1}, {1, 7}, {3, 7}, {8, 8}, {8, 9}, {16, 16}, {32, 1}, {64, 64}, {8, 4096}, {8, 65536}, {-1, 8}, {0, 0}} {
		b.Run(fmt.Sprintf("key=%d/value=%d", sizes[0], sizes[1]), func(b *testing.B) {
			input := record.Bytes{Topic: "out", Partition: 2, Offset: -1, Timestamp: 9}
			if sizes[0] >= 0 {
				input.Key = make([]byte, sizes[0])
			}
			input.Value = make([]byte, sizes[1])
			b.ReportAllocs()
			b.SetBytes(int64(len(input.Key) + len(input.Value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				retainedClone = input.Clone()
			}
			b.StopTimer()
			if retainedClone.Timestamp != 9 || len(retainedClone.Key) != len(input.Key) || len(retainedClone.Value) != len(input.Value) {
				b.Fatal("clone changed record")
			}
		})
	}
}
