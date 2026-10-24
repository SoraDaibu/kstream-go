package stream

import (
	"errors"
	"fmt"
	"sort"
)

type sourceTopic struct {
	name     string
	internal bool
}

func (s sourceTopic) topic(application string) string {
	if s.internal {
		return application + "-" + s.name + "-repartition"
	}
	return s.name
}

func validExchangeTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > 249 || topic == "." || topic == ".." {
		return false
	}
	for _, ch := range []byte(topic) {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}

func (t Topology) resolveTopics(application string) (inputs, exchanges []string, err error) {
	if len(t.sources) == 0 || application == "" {
		return nil, nil, errors.New("built topology and application ID required")
	}
	seen := make(map[string]bool, len(t.sourceTopics))
	for _, source := range t.sourceTopics {
		topic := source.topic(application)
		if seen[topic] {
			return nil, nil, fmt.Errorf("duplicate resolved source topic %q", topic)
		}
		seen[topic] = true
		inputs = append(inputs, topic)
		if source.internal {
			if !validExchangeTopic(topic) {
				return nil, nil, fmt.Errorf("invalid generated repartition topic %q", topic)
			}
			for _, sink := range t.sinkTopics {
				if sink == topic {
					return nil, nil, fmt.Errorf("user sink targets internal repartition topic %q", topic)
				}
			}
			exchanges = append(exchanges, topic)
		}
	}
	sort.Strings(inputs)
	sort.Strings(exchanges)
	return inputs, exchanges, nil
}

// InputTopics returns an owned sorted list of public and internal source topics.
// Provision all topics with equal partition counts for the current group worker.
func (t Topology) InputTopics(application string) ([]string, error) {
	inputs, _, err := t.resolveTopics(application)
	return inputs, err
}

// RepartitionTopics returns the owned sorted exchange list for Kafka Config.
// Provision these topics with delete retention and stable partition counts.
func (t Topology) RepartitionTopics(application string) ([]string, error) {
	_, exchanges, err := t.resolveTopics(application)
	return exchanges, err
}
