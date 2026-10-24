package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type groupAssignment struct {
	valid      bool
	partitions map[int32]bool
	err        error
}
type assignmentTracker struct {
	mu      sync.Mutex // Rebalance infrastructure only; processors never take this lock.
	topics  []string
	current atomic.Pointer[groupAssignment]
}

func (t *assignmentTracker) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	snapshot := &groupAssignment{valid: true, partitions: make(map[int32]bool)}
	counts := make(map[int32]int)
	known := make(map[string]bool, len(t.topics))
	for _, topic := range t.topics {
		known[topic] = true
	}
	for topic, partitions := range assigned {
		if !known[topic] {
			snapshot.err = fmt.Errorf("unexpected assigned topic %q", topic)
			break
		}
		seen := make(map[int32]bool, len(partitions))
		for _, partition := range partitions {
			if partition < 0 || seen[partition] {
				snapshot.err = errors.New("invalid source partition assignment")
				break
			}
			seen[partition] = true
			counts[partition]++
		}
	}
	for partition, count := range counts {
		if count != len(t.topics) {
			snapshot.err = errors.New("source partitions are not co-located; every group member must subscribe to the same topology")
			break
		}
		snapshot.partitions[partition] = true
	}
	t.current.Store(snapshot)
}
func (t *assignmentTracker) revoked(context.Context, *kgo.Client, map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.current.Store(&groupAssignment{})
}

func sourcePartitionCount(ctx context.Context, client *kgo.Client, topics []string, changelog string) (int32, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	for _, topic := range append(append([]string(nil), topics...), changelog) {
		name := topic
		entry := kmsg.NewMetadataRequestTopic()
		entry.Topic = &name
		request.Topics = append(request.Topics, entry)
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	counts := make(map[string]int32, len(response.Topics))
	for _, topic := range response.Topics {
		if topic.Topic == nil {
			return 0, errors.New("metadata omitted topic name")
		}
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return 0, fmt.Errorf("metadata for %s: %w", *topic.Topic, err)
		}
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return 0, err
			}
		}
		counts[*topic.Topic] = int32(len(topic.Partitions))
	}
	count := counts[topics[0]]
	if count < 1 {
		return 0, errors.New("source topics must be provisioned")
	}
	for _, topic := range topics {
		if counts[topic] != count {
			return 0, errors.New("group worker requires equal source partition counts")
		}
	}
	if counts[changelog] < count {
		return 0, errors.New("changelog has fewer partitions than sources")
	}
	return count, nil
}
