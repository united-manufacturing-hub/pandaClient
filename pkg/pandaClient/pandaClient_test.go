package pandaClient

import (
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"regexp"
	"testing"
	"time"
)

func initZAP() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
}

func TestConnect(t *testing.T) {
	initZAP()
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

func TestPandaClient_EnqueueMessage(t *testing.T) {
	initZAP()
	kafkaOptions := kafka.NewClientOptions{
		ListenTopicRegex: regexp.MustCompile(".+"),
		SenderTag: kafka.SenderTag{
			Enabled: false,
		},
		ConsumerName:            "test-consumer",
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

	for i := 0; i < 10; i++ {
		err := client.EnqueueMessage(kafka.Message{
			Topic: "test-topic",
			Value: []byte("test-value"),
		})
		if err != nil {
			t.Fatalf("EnqueueMessage failed: %v", err)
		}
	}
	zap.S().Debugf("Sleeping for 5 seconds...")
	time.Sleep(5 * time.Second)
	zap.S().Debugf("Done sleeping...")
	errK, errH := client.Close()
	if errK != nil {
		t.Fatalf("Close failed: %v", errK)
	}
	if errH != nil {
		t.Fatalf("Close failed: %v", errH)
	}
}

func TestPandaClient_EnqueueMessageForceHTTP(t *testing.T) {
	initZAP()
	kafkaOptions := kafka.NewClientOptions{
		ListenTopicRegex: regexp.MustCompile("test-topic"),
		SenderTag: kafka.SenderTag{
			Enabled: false,
		},
		ConsumerName:            "test-consumer",
		ClientID:                "test-client-id",
		Brokers:                 []string{"kafka1.mgmt-test.umh.app:0"},
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
	if kafkaConnected {
		t.Fatalf("Kafka connection succeeded: %v", kafkaConnectError)
	}
	if !httpConnected {
		t.Fatalf("HTTP connection failed: %v", httpConnectError)
	}

	for i := 0; i < 10; i++ {
		err := client.EnqueueMessage(kafka.Message{
			Topic: "test-topic",
			Value: []byte("test-value"),
		})
		if err != nil {
			t.Fatalf("EnqueueMessage failed: %v", err)
		}
	}
	zap.S().Debugf("Sleeping for 5 seconds...")
	time.Sleep(5 * time.Second)
	zap.S().Debugf("Done sleeping...")

	// Listen for messages
	zap.S().Debugf("Listening for messages")
	var timedOut bool
	for !timedOut {
		select {
		case msg := <-client.GetMessages():
			{

				zap.S().Debugf("Received message: %v", msg)
			}
		case timeout := <-time.After(10 * time.Second):
			{
				zap.S().Debugf("Timeout: %v", timeout)
				timedOut = true
				break
			}
		}
	}

	errK, errH := client.Close()
	if errK != nil {
		t.Fatalf("Close failed: %v", errK)
	}
	if errH != nil {
		t.Fatalf("Close failed: %v", errH)
	}
}
