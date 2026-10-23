// Package serde provides explicit boundary codecs. Encodings must remain stable
// across task restarts. Nil bytes are reserved for Kafka nulls.
package serde

import (
 "encoding/binary"
 "encoding/json"
 "errors"
)

type Codec[T any] struct {
 Encode func(T) ([]byte,error)
 Decode func([]byte) (T,error)
}
func (c Codec[T]) Valid() bool { return c.Encode != nil && c.Decode != nil }

var String = Codec[string]{
 Encode: func(v string)([]byte,error) { b:=make([]byte,len(v));copy(b,v);return b,nil },
 Decode: func(b []byte)(string,error) { return string(b),nil },
}
var Int64 = Codec[int64]{
 Encode: func(v int64)([]byte,error) { b:=make([]byte,8);binary.BigEndian.PutUint64(b,uint64(v));return b,nil },
 Decode: func(b []byte)(int64,error) { if len(b)!=8{return 0,errors.New("int64 requires eight bytes")};return int64(binary.BigEndian.Uint64(b)),nil },
}
func JSON[T any]() Codec[T] { return Codec[T]{
 Encode: func(v T)([]byte,error){return json.Marshal(v)},
 Decode: func(b []byte)(T,error){var v T;err:=json.Unmarshal(b,&v);return v,err},
} }
