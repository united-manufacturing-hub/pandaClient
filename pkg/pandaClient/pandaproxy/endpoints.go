package pandaproxy

import (
	"bytes"
	"fmt"
	"github.com/goccy/go-json"
	"go.uber.org/zap"
	"io"
	"net/http"
)

func DoT[T any](baseUrl, path string, body io.Reader, method string, ct contentType, noResponseExpected bool, acceptType *contentType) (*T, *ErrorBody, error) {
	// prepend the path with a slash if it doesn't have one
	if path[0] != '/' {
		path = "/" + path
	}
	response, err := DoRequest(method, baseUrl+path, body, ct, acceptType)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()

	var bodyBytes []byte
	// read all response body bytes
	bodyBytes, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Parse error body
		var errorBody ErrorBody
		err = json.Unmarshal(bodyBytes, &errorBody)
		if err != nil {
			return nil, nil, err
		}
		if errorBody.Code == 0 {
			errorBody.Code = int64(response.StatusCode)
		}
		return nil, &errorBody, nil
	}

	if noResponseExpected {
		return nil, nil, nil
	}

	var responseJ T
	err = json.Unmarshal(bodyBytes, &responseJ)
	if err != nil {
		zap.S().Errorf("Error unmarshalling response: %v", err)
		zap.S().Errorf("Response body: %s", string(bodyBytes))
		zap.S().Errorf("T type: %T", responseJ)
		return nil, nil, err
	}

	return &responseJ, nil, nil
}

func GetT[T any](baseUrl, path string, body io.Reader, ct contentType, noResponseExpected bool, acceptType *contentType) (*T, *ErrorBody, error) {
	return DoT[T](baseUrl, path, body, http.MethodGet, ct, noResponseExpected, acceptType)
}

func PostT[T any](baseUrl, path string, body io.Reader, ct contentType, noResponseExpected bool, acceptType *contentType) (*T, *ErrorBody, error) {
	return DoT[T](baseUrl, path, body, http.MethodPost, ct, noResponseExpected, acceptType)
}

func DeleteT[T any](baseUrl, path string, body io.Reader, ct contentType, noResponseExpected bool, acceptType *contentType) (*T, *ErrorBody, error) {
	return DoT[T](baseUrl, path, body, http.MethodDelete, ct, noResponseExpected, acceptType)
}

func GetBrokers(baseUrl string) (*Brokers, *ErrorBody, error) {
	return GetT[Brokers](baseUrl, "/brokers", nil, ContentTypeJSON, false, nil)
}

func GetTopics(baseUrl string) (*Topics, *ErrorBody, error) {
	return GetT[Topics](baseUrl, "/topics", nil, ContentTypeJSON, false, nil)
}

func PostConsumerGroup(baseUrl, groupName, consumerName string) (*ConsumerGroupInstance, *ErrorBody, error) {
	group := ConsumerGroup{
		Format:                   "json",
		Name:                     consumerName,
		AutoOffsetReset:          "earliest",
		AutoCommitEnable:         "false",
		FetchMinBytes:            "1",
		ConsumerRequestTimeoutMS: "30000",
	}

	groupBytes, err := json.Marshal(group)
	if err != nil {
		return nil, nil, err
	}
	// Create a reader from the bytes
	msgReader := bytes.NewReader(groupBytes)

	return PostT[ConsumerGroupInstance](baseUrl, fmt.Sprintf("/consumers/%s", groupName), msgReader, ContentTypeJSON, false, nil)
}

func DeleteConsumerGroupInstance(baseUrl, groupName string, instanceId string) (*ErrorBody, error) {
	_, e, err := DeleteT[any](baseUrl, fmt.Sprintf("/consumers/%s/instances/%s", groupName, instanceId), nil, ContentTypeJSON, true, nil)
	return e, err
}

func PostSubscribeToTopic(baseUrl, groupName, instanceId string, topics SubscribeTopics) (*ErrorBody, error) {
	topicBytes, err := json.Marshal(topics)
	if err != nil {
		return nil, err
	}
	// Create a reader from the bytes
	msgReader := bytes.NewReader(topicBytes)

	var e *ErrorBody
	_, e, err = PostT[any](baseUrl, fmt.Sprintf("/consumers/%s/instances/%s/subscription", groupName, instanceId), msgReader, ContentTypeJSON, true, nil)
	return e, err
}

func PostMessages(baseUrl, topic string, messages Messages) (*MessageOffsets, *ErrorBody, error) {
	msgBytes, err := json.Marshal(messages)
	if err != nil {
		return nil, nil, err
	}
	// Create a reader from the bytes
	msgReader := bytes.NewReader(msgBytes)

	return PostT[MessageOffsets](baseUrl, fmt.Sprintf("/topics/%s", topic), msgReader, ContentTypeJSONJSON, false, nil)
}

func GetMessages(baseUrl, groupName, instanceId string) (*[]RecordEx, *ErrorBody, error) {
	var at = ContentTypeJSONJSON
	return GetT[[]RecordEx](baseUrl, fmt.Sprintf("/consumers/%s/instances/%s/records", groupName, instanceId), nil, ContentTypeJSONJSON, false, &at)
}

func PostCommitOffsets(baseUrl, groupName, instanceId string, p Partitions) (interface{}, *ErrorBody, error) {
	partitionsBytes, err := json.Marshal(p)
	if err != nil {
		return nil, nil, err
	}
	// Create a reader from the bytes
	msgReader := bytes.NewReader(partitionsBytes)

	return PostT[any](baseUrl, fmt.Sprintf("/consumers/%s/instances/%s/offsets", groupName, instanceId), msgReader, ContentTypeJSON, false, nil)
}
