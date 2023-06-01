package pandaproxy

import (
	"context"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type contentType string

const (
	ContentTypeJSONJSON contentType = "application/vnd.kafka.json.v2+json"
	ContentTypeJSON     contentType = "application/vnd.kafka.v2+json"
)

func DoRequest(method, url string, body io.Reader, ct contentType, acceptType *contentType) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		zap.S().Errorf("Error creating request: %v", err)
		return nil, err
	}
	request.Header.Set("Content-Type", string(ct))
	if acceptType != nil {
		request.Header.Set("Accept", string(*acceptType))
	}

	var response *http.Response
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		zap.S().Errorf("Error sending request: %v", err)
		return nil, err
	}
	return response, nil
}

func KafkaToHTTPMessage(message kafka.Message) Record {
	var record Record
	if message.Key != nil && len(message.Key) > 0 {
		record.Key = string(message.Key)
	}
	if message.Value != nil && len(message.Value) > 0 {
		record.Value = string(message.Value)
	}
	record.Partition = 0
	return record
}

func HTTPToKafkaMessage(message RecordEx) (messageK kafka.Message) {
	if len(message.Key) > 0 {
		messageK.Key = []byte(message.Key)
	}
	if len(message.Value) > 0 {
		messageK.Value = []byte(message.Value)
	}
	messageK.Topic = message.Topic
	return messageK
}

func contains[T comparable](topics []T, topic T) bool {
	for _, t := range topics {
		if t == topic {
			return true
		}
	}
	return false
}
