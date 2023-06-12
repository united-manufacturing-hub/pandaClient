package pandaproxy

import (
	"context"
	"github.com/goccy/go-json"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"io"
	"net/http"
	"time"
)

type contentType string

const (
	ContentTypeJSONJSON contentType   = "application/vnd.kafka.json.v2+json"
	ContentTypeJSON     contentType   = "application/vnd.kafka.v2+json"
	RequestTimeout      time.Duration = 30 * time.Second
)

func DoRequest(method, url string, body io.Reader, ct contentType, acceptType *contentType) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		zap.S().Errorf("Error creating request: %v", err)
		return nil, 0, err
	}
	request.Header.Set("Content-Type", string(ct))
	if acceptType != nil {
		request.Header.Set("Accept", string(*acceptType))
	}

	client := &http.Client{
		Timeout: RequestTimeout,
	}
	response, err := client.Do(request)
	if err != nil {
		zap.S().Errorf("Error sending request: %v", err)
		return nil, 0, err
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, err
	}

	return bodyBytes, response.StatusCode, nil
}

func KafkaToHTTPMessage(message kafka.Message) Record {
	var record Record
	if message.Key != nil && len(message.Key) > 0 {
		record.Key = string(message.Key)
	}
	if message.Value != nil && len(message.Value) > 0 {
		var value map[string]interface{}
		err := json.Unmarshal(message.Value, &value)
		if err != nil {
			record.Value = string(message.Value)
		} else {
			record.Value = value
		}
	}
	record.Partition = 0
	return record
}

func HTTPToKafkaMessage(message RecordEx) (kafka.Message, error) {
	var messageK kafka.Message
	if len(message.Key) > 0 {
		messageK.Key = []byte(message.Key)
	}
	if message.Value != nil {
		bytes, err := json.Marshal(message.Value)
		if err != nil {
			zap.S().Errorf("Error marshalling message value: %v", err)
			return messageK, err
		}
		messageK.Value = bytes
	}
	messageK.Topic = message.Topic
	return messageK, nil
}

func contains[T comparable](topics []T, topic T) bool {
	for _, t := range topics {
		if t == topic {
			return true
		}
	}
	return false
}
