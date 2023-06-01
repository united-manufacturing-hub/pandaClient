package pandaproxy

import (
	"fmt"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"regexp"
	"sync/atomic"
	"time"
)

type MessageTopic struct {
	Topic   string
	Message Record
}

type HTTPMessageQueue struct {
	messageQueue     chan MessageTopic
	incomingMessages chan kafka.Message
	groupInstance    *ConsumerGroupInstance
	baseUrl          string
	groupName        string
	closing          atomic.Bool
	sentMessages     atomic.Uint64
	receivedMessages atomic.Uint64
	sentBytes        atomic.Uint64
	receivedBytes    atomic.Uint64
}

func New(baseUrl string) HTTPMessageQueue {
	return HTTPMessageQueue{
		messageQueue:     make(chan MessageTopic, 1000),
		baseUrl:          baseUrl,
		incomingMessages: make(chan kafka.Message, 1000),
	}
}

func (h *HTTPMessageQueue) EnqueueMessage(message kafka.Message) error {
	h.messageQueue <- MessageTopic{
		Message: KafkaToHTTPMessage(message),
		Topic:   message.Topic,
	}
	return nil
}

func (h *HTTPMessageQueue) StartMessageSender() {
	var topicMessageMap = make(map[string][]Record)
	for !h.closing.Load() {
		select {
		case messageTopic := <-h.messageQueue:
			{
				if _, ok := topicMessageMap[messageTopic.Topic]; !ok {
					topicMessageMap[messageTopic.Topic] = make([]Record, 0, 1)
				}
				topicMessageMap[messageTopic.Topic] = append(topicMessageMap[messageTopic.Topic], messageTopic.Message)
			}
		case <-time.After(1 * time.Second):
			{
				for topic, messages := range topicMessageMap {
					if len(messages) == 0 {
						continue
					}
					_, errorBody, err := PostMessages(h.baseUrl, topic, Messages{Records: messages})
					if err != nil {
						zap.S().Warnf("Error posting messages to topic %s: %v", topic, err)
						continue
					}
					if errorBody != nil {
						zap.S().Warnf("Error posting messages to topic %s: %v", topic, errorBody)
						continue
					}
					h.sentMessages.Add(uint64(len(messages)))
					for _, message := range messages {
						h.sentBytes.Add(uint64(len(message.Value)))
						h.sentBytes.Add(uint64(len(message.Key)))
					}
				}
				topicMessageMap = make(map[string][]Record)
			}
		}
	}
}

func (h *HTTPMessageQueue) Close() error {
	h.closing.Store(true)
	if h.groupInstance != nil {
		_, err := DeleteConsumerGroupInstance(h.baseUrl, h.groupName, h.groupInstance.InstanceID)
		return err
	}
	return nil
}

func (h *HTTPMessageQueue) GetMessages() <-chan kafka.Message {
	return h.incomingMessages
}

func (h *HTTPMessageQueue) StartSubscriber(clientId, consumerName string, listenRegex *regexp.Regexp) error {
	instance, bodyError, err := PostConsumerGroup(h.baseUrl, consumerName, clientId)
	if err != nil {
		return err
	}
	if bodyError != nil && bodyError.Code != 409 {
		return fmt.Errorf("error creating consumer group: %v", bodyError)
	}

	// If bodyError is 409, we need to delete the instance and try again
	if bodyError != nil && bodyError.Code == 409 {
		zap.S().Debugf("Consumer group already exists, deleting and recreating")
		bodyError, err = DeleteConsumerGroupInstance(h.baseUrl, consumerName, clientId)
		if err != nil {
			return err
		}
		if bodyError != nil {
			return fmt.Errorf("error deleting consumer group: %v", bodyError)
		}
		zap.S().Debugf("Deleted consumer group, recreating")

		instance, bodyError, err = PostConsumerGroup(h.baseUrl, consumerName, clientId)
		if err != nil {
			return err
		}
		if bodyError != nil {
			return fmt.Errorf("error creating consumer group: %v", bodyError)
		}
	}
	zap.S().Debugf("Created consumer group (%s) instance: %+v", consumerName, instance)

	h.groupName = consumerName
	h.groupInstance = instance

	go h.topicRefresher(listenRegex)
	go h.consume()

	return nil
}

func (h *HTTPMessageQueue) topicRefresher(regex *regexp.Regexp) {
	zap.S().Infof("Starting topic refresher")
	var previousTopics []string
	for !h.closing.Load() {
		topics, e, err := GetTopics(h.baseUrl)
		if err != nil || e != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		var topicList []string
		for _, topic := range *topics {
			if regex.MatchString(topic) {
				topicList = append(topicList, topic)
			}
		}
		// Get diff between previous and current topics
		var newTopics []string
		for _, topic := range topicList {
			if !contains(previousTopics, topic) {
				newTopics = append(newTopics, topic)
			}
		}
		if len(newTopics) == 0 {
			time.Sleep(10 * time.Second)
			continue
		}
		zap.S().Infof("Subscribing to new topics: %+v", newTopics)

		var subtopics = SubscribeTopics{
			Topics: newTopics,
		}
		topicErr, err := PostSubscribeToTopic(h.baseUrl, h.groupName, h.groupInstance.InstanceID, subtopics)
		if err != nil {
			zap.S().Warnf("Error subscribing to topic: %v", err)
		}
		if topicErr != nil {
			zap.S().Warnf("Error subscribing to topic (TE): %v", topicErr)
		}
		previousTopics = topicList
		time.Sleep(5 * time.Second)
	}
	zap.S().Infof("Closing topic refresher")
}

func (h *HTTPMessageQueue) consume() {
	zap.S().Infof("Starting consumer loop")
	var messages *[]RecordEx
	var bodyError *ErrorBody
	var err error
	for !h.closing.Load() {
		messages, bodyError, err = GetMessages(h.baseUrl, h.groupName, h.groupInstance.InstanceID)
		if err != nil {
			zap.S().Debugf("Error getting messages: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if bodyError != nil {
			zap.S().Debugf("Error getting messages: %#v", bodyError)
			time.Sleep(1 * time.Second)
			continue
		}
		if messages == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		zap.S().Debugf("Got %d messages", len(*messages))

		var partitionList = make([]Partition, 0, 1)

		for _, message := range *messages {
			partitionList = append(partitionList, Partition{
				Partition: message.Partition,
				Offset:    message.Offset,
				Topic:     message.Topic,
			})
			h.receivedMessages.Add(1)
			h.receivedBytes.Add(uint64(len(message.Value)))
			h.receivedBytes.Add(uint64(len(message.Key)))
			h.incomingMessages <- HTTPToKafkaMessage(message)
		}
		var partitions = Partitions{
			Partitions: partitionList,
		}

		if len(partitions.Partitions) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		_, bodyError, err = PostCommitOffsets(h.baseUrl, h.groupName, h.groupInstance.InstanceID, partitions)
		if err != nil {
			zap.S().Debugf("Error committing offsets: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if bodyError != nil {
			zap.S().Debugf("Error committing offsets: %v", bodyError)
			time.Sleep(1 * time.Second)
			continue
		}

		time.Sleep(100 * time.Millisecond)
	}
	zap.S().Infof("Consumer loop closed")
}

func (h *HTTPMessageQueue) GetQueueLength() int {
	return len(h.messageQueue)
}

func (h *HTTPMessageQueue) Ready() bool {
	return !h.closing.Load()
}

func (h *HTTPMessageQueue) Closed() bool {
	return h.closing.Load()
}

func (h *HTTPMessageQueue) GetStats() (sent uint64, received uint64, sendBytesA uint64, receivedBytesA uint64) {
	return h.sentMessages.Load(), h.receivedMessages.Load(), h.sentBytes.Load(), h.receivedBytes.Load()
}
