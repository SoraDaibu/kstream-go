// Package processor is the lower-level typed API underlying the stream DSL.
package processor

import "github.com/SoraDaibu/kstream-go/record"

type Handler[K,V any] func(record.Record[K,V]) error
type Forward[K,V any] func(record.Record[K,V]) error
// Context is valid during an input transaction. Returned state bytes are copies.
// Stores belong exclusively to the current task. Scan order is unspecified.
type Context interface {
 StreamTime() int64
 StoreGet(store,key string) ([]byte,bool)
 StorePut(store,key string,value []byte)
 StoreScan(store string,visit func(string,[]byte))
}
// Factory is invoked once per task, not once per record.
type Factory[K,V any] func(Context,Forward[K,V]) Handler[K,V]
