package record_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/SoraDaibu/kstream-go/record"
)

func cloneInput(size int, fill byte) []byte {
	if size < 0 {
		return nil
	}
	return bytes.Repeat([]byte{fill}, size)
}

func TestBytesClonePreservesMetadataNullsEmptyAndOwnership(t *testing.T) {
	for _, keySize := range []int{-1, 0, 1, 7, 8, 9, 15, 16, 17, 64, 4096} {
		for _, valueSize := range []int{-1, 0, 1, 7, 8, 9, 15, 16, 17, 64, 4096} {
			t.Run(fmt.Sprintf("key=%d/value=%d", keySize, valueSize), func(t *testing.T) {
				input := record.Bytes{Topic: "topic", Partition: 3, Offset: 17, Timestamp: 91, Key: cloneInput(keySize, 0x21), Value: cloneInput(valueSize, 0x42)}
				out := input.Clone()
				if out.Topic != input.Topic || out.Partition != input.Partition || out.Offset != input.Offset || out.Timestamp != input.Timestamp {
					t.Fatal("metadata changed", out)
				}
				if !bytes.Equal(out.Key, input.Key) || !bytes.Equal(out.Value, input.Value) || (out.Key == nil) != (input.Key == nil) || (out.Value == nil) != (input.Value == nil) {
					t.Fatal("payload/null identity changed")
				}
				if cap(out.Key) != len(out.Key) || cap(out.Value) != len(out.Value) {
					t.Fatal("clone exposes writable capacity beyond a payload")
				}
				if len(input.Key) > 0 {
					input.Key[0] = 0x99
					if out.Key[0] != 0x21 {
						t.Fatal("key aliases input")
					}
				}
				if len(input.Value) > 0 {
					input.Value[0] = 0x99
					if out.Value[0] != 0x42 {
						t.Fatal("value aliases input")
					}
				}
				out.Key = append(out.Key, 0x88)
				if len(out.Value) > 0 && out.Value[0] != 0x42 {
					t.Fatal("appended key overwrote value")
				}
				out.Value = append(out.Value, 0x77)
				if out.Key[len(out.Key)-1] != 0x88 {
					t.Fatal("appended value overwrote key")
				}
			})
		}
	}
}

func TestBytesCloneSeparatesOverlappingInputPayloads(t *testing.T) {
	shared := []byte{1, 2, 3, 4}
	input := record.Bytes{Key: shared[:3], Value: shared[1:]}
	out := input.Clone()
	out.Key[1] = 9
	if out.Value[0] != 2 || shared[1] != 2 {
		t.Fatal("cloned payloads or source share writable bytes")
	}
	out.Value[1] = 8
	if out.Key[2] != 3 || shared[2] != 3 {
		t.Fatal("cloned value overwrote key or source")
	}
}
