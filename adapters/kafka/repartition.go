package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func validateExchangePolicies(ctx context.Context, client kmsg.Requestor, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	request := kmsg.NewPtrDescribeConfigsRequest()
	want := make(map[string]bool, len(topics))
	for _, topic := range topics {
		want[topic] = true
		request.Resources = append(request.Resources, kmsg.DescribeConfigsRequestResource{
			ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: topic, ConfigNames: []string{"cleanup.policy"},
		})
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("missing repartition topic configuration response")
	}
	seen := make(map[string]bool, len(topics))
	for _, resource := range response.Resources {
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic || !want[resource.ResourceName] || seen[resource.ResourceName] {
			return errors.New("unexpected or duplicate repartition topic configuration")
		}
		if err := kerr.ErrorForCode(resource.ErrorCode); err != nil {
			return fmt.Errorf("repartition configuration for %s: %w", resource.ResourceName, err)
		}
		valid := false
		for _, config := range resource.Configs {
			if config.Name != "cleanup.policy" {
				continue
			}
			if valid || config.Value == nil || strings.TrimSpace(*config.Value) != "delete" {
				return fmt.Errorf("repartition topic %q requires cleanup.policy=delete without compaction", resource.ResourceName)
			}
			valid = true
		}
		if !valid {
			return fmt.Errorf("repartition topic %q requires cleanup.policy=delete without compaction", resource.ResourceName)
		}
		seen[resource.ResourceName] = true
	}
	if len(seen) != len(want) {
		return errors.New("broker omitted repartition topic configuration")
	}
	return nil
}

func (c Config) validateRepartitionTopics(topics []string) error {
	for _, topic := range topics {
		found := false
		for _, configured := range c.RepartitionTopics {
			if configured == topic {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("repartition topic %q missing from Kafka Config.RepartitionTopics", topic)
		}
	}
	return nil
}

func (b *Backend) ValidateRepartitionTopics(topics []string) error {
	if b == nil {
		return errors.New("nil Kafka backend")
	}
	return b.config.validateRepartitionTopics(topics)
}

func (b *groupBackend) ValidateRepartitionTopics(topics []string) error {
	if b == nil {
		return errors.New("nil Kafka group backend")
	}
	return b.config.validateRepartitionTopics(topics)
}
