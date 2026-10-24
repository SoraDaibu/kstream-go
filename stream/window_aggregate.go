package stream

import (
	"errors"

	"github.com/SoraDaibu/kstream-go/serde"
	"github.com/SoraDaibu/kstream-go/window"
)

// WindowedAggregate applies typed aggregate state in tumbling windows. Null keys
// skip; null values reach aggregator with inputNull=true and advance the clock.
// An absent aggregate runs initializer before aggregator. Each bool denotes a
// Kafka null; a null result deletes state and emits a tombstone.
func WindowedAggregate[K, V, A any](s KStream[K, V], store string, w window.Tumbling, value serde.Codec[A],
	initializer func() (A, bool), aggregator func(key K, input V, inputNull bool, old A, oldNull bool) (A, bool),
) KStream[window.Key[K], A] {
	return timeWindowedAggregate(s, store, window.Hopping{Size: w.Size, Advance: w.Size, Grace: w.Grace}, value, initializer, aggregator)
}

// HoppingWindowedAggregate applies the same rules to each open overlapping
// window, in ascending window-start order. Result serialization is explicit.
func HoppingWindowedAggregate[K, V, A any](s KStream[K, V], store string, w window.Hopping, value serde.Codec[A],
	initializer func() (A, bool), aggregator func(key K, input V, inputNull bool, old A, oldNull bool) (A, bool),
) KStream[window.Key[K], A] {
	return timeWindowedAggregate(s, store, w, value, initializer, aggregator)
}

func timeWindowedAggregate[K, V, A any](s KStream[K, V], store string, w window.Hopping, value serde.Codec[A],
	initializer func() (A, bool), aggregator func(K, V, bool, A, bool) (A, bool),
) KStream[window.Key[K], A] {
	s.builder.mutable()
	if initializer == nil || aggregator == nil {
		s.builder.fail(errors.New("windowed aggregate requires initializer and aggregator"))
	}
	return timeWindowedFold(s, store, w, "window-aggregate", value,
		func(key K, input V, inputNull bool, old A, present bool) (A, bool) {
			oldNull := false
			if !present {
				old, oldNull = initializer()
			}
			return aggregator(key, input, inputNull, old, oldNull)
		})
}
