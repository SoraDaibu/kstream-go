package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestInternalRepartitionRoutingOverridesOrdinaryCustomPartitioner(t *testing.T) {
	p := newTopicPartitioner(Config{ChangelogTopic: "state", RepartitionTopics: []string{"exchange"}}, kgo.ManualPartitioner())
	for _, count := range []int{1, 3, 7} {
		actual := p.ForTopic("exchange")
		reference := kgo.StickyKeyPartitioner(nil).ForTopic("exchange")
		for _, key := range [][]byte{[]byte("a"), []byte("b"), {}} {
			row := &kgo.Record{Key: key, Partition: int32(count + 1)}
			if got, want := actual.Partition(row, count), reference.Partition(row, count); got != want {
				t.Fatal("exchange did not use Kafka keyed hashing", key, count, got, want)
			}
		}
		if got := actual.Partition(&kgo.Record{Partition: -1}, count); got < 0 || got >= count {
			t.Fatal("null key did not receive a valid partition", got)
		}
	}
	if got := p.ForTopic("ordinary").Partition(&kgo.Record{Partition: 2}, 3); got != 2 {
		t.Fatal("ordinary custom partitioning changed", got)
	}
	if got := p.ForTopic("state").Partition(&kgo.Record{Partition: 1}, 3); got != 1 {
		t.Fatal("changelog lost task-local partition routing", got)
	}
}

type exchangeConfigRequestor func(context.Context, kmsg.Request) (kmsg.Response, error)

func (f exchangeConfigRequestor) Request(ctx context.Context, request kmsg.Request) (kmsg.Response, error) {
	return f(ctx, request)
}

func TestExchangePolicyRequiresUncompactedDeleteTopic(t *testing.T) {
	for _, policy := range []string{"delete", "compact", "delete,compact", ""} {
		requestor := exchangeConfigRequestor(func(_ context.Context, raw kmsg.Request) (kmsg.Response, error) {
			request := raw.(*kmsg.DescribeConfigsRequest)
			if len(request.Resources) != 1 || request.Resources[0].ResourceName != "exchange" || len(request.Resources[0].ConfigNames) != 1 || request.Resources[0].ConfigNames[0] != "cleanup.policy" {
				t.Fatal("configuration request exceeded exchange policy scope", request)
			}
			return &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{
				ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "exchange",
				Configs: []kmsg.DescribeConfigsResponseResourceConfig{{Name: "cleanup.policy", Value: &policy}},
			}}}, nil
		})
		err := validateExchangePolicies(context.Background(), requestor, []string{"exchange"})
		if (err == nil) != (policy == "delete") {
			t.Fatal("unsafe repartition cleanup policy accepted", policy, err)
		}
	}
	denied := exchangeConfigRequestor(func(context.Context, kmsg.Request) (kmsg.Response, error) {
		return &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{
			ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "exchange", ErrorCode: kerr.TopicAuthorizationFailed.Code,
		}}}, nil
	})
	if err := validateExchangePolicies(context.Background(), denied, []string{"exchange"}); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatal("broker configuration authorization error lost", err)
	}
	unused := exchangeConfigRequestor(func(context.Context, kmsg.Request) (kmsg.Response, error) {
		t.Fatal("ordinary topology required exchange configuration RPC")
		return nil, nil
	})
	if err := validateExchangePolicies(context.Background(), unused, nil); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaExchangeConfigurationIsOwnedAndValidated(t *testing.T) {
	topics := []string{"exchange"}
	backend, err := New(Config{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RepartitionTopics: topics})
	if err != nil {
		t.Fatal(err)
	}
	topics[0] = "mutated"
	if err := backend.ValidateRepartitionTopics([]string{"exchange"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.ValidateRepartitionTopics([]string{"mutated"}); err == nil {
		t.Fatal("adapter retained caller's mutable exchange list")
	}
	for _, invalid := range [][]string{{""}, {"state"}, {"exchange", "exchange"}} {
		if _, err := New(Config{Brokers: []string{"unused:9092"}, ChangelogTopic: "state", RepartitionTopics: invalid}); err == nil {
			t.Fatal("invalid exchange configuration accepted", invalid)
		}
	}
}
