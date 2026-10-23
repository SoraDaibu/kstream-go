// Package record defines typed records and Kafka boundary records.
package record

// Record represents an immutable event. Timestamp is Unix milliseconds.
// Null flags distinguish Kafka nulls from ordinary Go zero values.
type Record[K, V any] struct {
	Key       K
	Value     V
	Timestamp int64
	NullKey   bool
	NullValue bool
}

// Bytes is an infrastructure record. Nil Key/Value mean Kafka nulls.
// Offset is the consumed offset; output offsets are assigned by the broker.
type Bytes struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	v := make([]byte, len(b))
	copy(v, b)
	return v
}
func (r Bytes) Clone() Bytes { r.Key = CloneBytes(r.Key); r.Value = CloneBytes(r.Value); return r }
