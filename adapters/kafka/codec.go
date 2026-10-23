package kafka

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/SoraDaibu/kstream-go/execution"
)

const (
	keyVersion  byte = 1
	barrierKind byte = iota
	metadataKind
	stateKind
)

type logKey struct {
	kind                          byte
	application, task, store, key string
	partition                     int32
}

func appendField(dst []byte, field string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(field)))
	return append(dst, field...)
}
func encodeKey(kind byte, s execution.Spec, store, key string) []byte {
	out := []byte{keyVersion, kind}
	out = appendField(out, s.ApplicationID)
	out = appendField(out, s.ID)
	out = binary.BigEndian.AppendUint32(out, uint32(s.Partition))
	if kind == stateKind {
		out = appendField(out, store)
		out = appendField(out, key)
	} else if kind == barrierKind {
		out = appendField(out, key)
	}
	return out
}
func decodeKey(raw []byte) (logKey, error) {
	if len(raw) < 2 || raw[0] != keyVersion {
		return logKey{}, errors.New("unsupported changelog key format")
	}
	key := logKey{kind: raw[1]}
	offset := 2
	read := func() (string, error) {
		if offset >= len(raw) {
			return "", errors.New("truncated changelog key")
		}
		size, n := binary.Uvarint(raw[offset:])
		if n <= 0 {
			return "", errors.New("invalid changelog key length")
		}
		offset += n
		if size > uint64(len(raw)-offset) {
			return "", errors.New("truncated changelog key field")
		}
		value := string(raw[offset : offset+int(size)])
		offset += int(size)
		return value, nil
	}
	var err error
	if key.application, err = read(); err != nil {
		return logKey{}, err
	}
	if key.task, err = read(); err != nil {
		return logKey{}, err
	}
	if len(raw)-offset < 4 {
		return logKey{}, errors.New("missing changelog partition scope")
	}
	partition := binary.BigEndian.Uint32(raw[offset:])
	offset += 4
	if partition > math.MaxInt32 {
		return logKey{}, errors.New("invalid changelog partition scope")
	}
	key.partition = int32(partition)
	switch key.kind {
	case stateKind:
		if key.store, err = read(); err != nil {
			return logKey{}, err
		}
		if key.key, err = read(); err != nil {
			return logKey{}, err
		}
	case barrierKind:
		if key.key, err = read(); err != nil {
			return logKey{}, err
		}
	case metadataKind:
	default:
		return logKey{}, errors.New("unknown changelog record kind")
	}
	if offset != len(raw) {
		return logKey{}, errors.New("trailing changelog key bytes")
	}
	return key, nil
}
