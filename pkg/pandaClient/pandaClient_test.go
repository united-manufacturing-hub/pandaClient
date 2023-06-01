package pandaClient

import (
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"regexp"
	"testing"
)

func TestConnect(t *testing.T) {

	kafkaOptions := kafka.NewClientOptions{
		ListenTopicRegex: regexp.MustCompile(".+"),
		SenderTag: kafka.SenderTag{
			Enabled: false,
		},
		ConsumerName:            "test-consumer",
		TransactionalID:         "test-transactional-id",
		ClientID:                "test-client-id",
		Brokers:                 []string{"kafka1.mgmt-test.umh.app:31092"},
		StartOffset:             0,
		OpenDeadLine:            0,
		Partitions:              1,
		ReplicationFactor:       1,
		EnableTLS:               false,
		AutoCommit:              true,
		ProducerReturnSuccesses: false,
	}

	httpOptions := &HTTPClientOptions{
		baseURL: "https://kafka1.mgmt-test.umh.app",
	}

	client := New(kafkaOptions, httpOptions)
	kafkaConnected, httpConnected, kafkaConnectError, httpConnectError := client.Connect()
	if !kafkaConnected {
		t.Fatalf("Kafka connection failed: %v", kafkaConnectError)
	}
	if !httpConnected {
		t.Fatalf("HTTP connection failed: %v", httpConnectError)
	}
}
