package pandaproxy

import (
	"fmt"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"regexp"
	"sync/atomic"
	"time"
)

//goland:noinspection GoUnitSpecificDurationSuffix
const (
	FiveSeconds            = 5 * time.Second
	OneHundredMilliseconds = 100 * time.Millisecond
	QueueSize              = 1000
)

type MessageTopic struct {
	Topic   string
	Message Record
}

type HTTPMessageQueue struct {
	messageQueue           chan MessageTopic
	incomingMessages       chan kafka.Message
	groupInstance          *ConsumerGroupInstance
	baseUrl                string
	groupName              string
	sentMessages           atomic.Uint64
	receivedMessages       atomic.Uint64
	sentBytes              atomic.Uint64
	receivedBytes          atomic.Uint64
	messageQueueClosed     atomic.Bool
	incomingMessagesClosed atomic.Bool
	closing                atomic.Bool
}

func New(baseUrl string) HTTPMessageQueue {
	return HTTPMessageQueue{
		messageQueue:           make(chan MessageTopic, QueueSize),
		incomingMessages:       make(chan kafka.Message, QueueSize),
		messageQueueClosed:     atomic.Bool{},
		incomingMessagesClosed: atomic.Bool{},
		groupInstance:          nil,
		baseUrl:                baseUrl,
		groupName:              "",
		closing:                atomic.Bool{},
		sentMessages:           atomic.Uint64{},
		receivedMessages:       atomic.Uint64{},
		sentBytes:              atomic.Uint64{},
		receivedBytes:          atomic.Uint64{},
	}
}

func (h *HTTPMessageQueue) EnqueueMessage(message kafka.Message) error {
	if h.messageQueueClosed.Load() {
		return fmt.Errorf("message queue is closed")
	}
	h.messageQueue <- MessageTopic{
		Message: KafkaToHTTPMessage(message),
		Topic:   message.Topic,
	}
	return nil
}

func (h *HTTPMessageQueue) StartMessageSender() {
	var topicMessageMap = make(map[string][]Record)
	var topicFailureMap = make(map[string]uint64)
	for !h.closing.Load() && !h.messageQueueClosed.Load() {
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
					_, errorBody, err, sendMsg, sendBytes := PostMessages(h.baseUrl, topic, Messages{Records: messages})
					if err != nil {
						if _, ok := topicFailureMap[topic]; !ok {
							topicFailureMap[topic] = 0
						}
						topicFailureMap[topic]++

						zap.S().Warnf("Error posting messages to topic %s: %v (%d failures for this Topic)", topic, err, topicFailureMap[topic])
						continue
					}
					if errorBody != nil {
						zap.S().Warnf("Error posting messages to topic %s: %v (%d failures for this Topic)", topic, errorBody, topicFailureMap[topic])
						continue
					}
					h.sentMessages.Add(uint64(sendMsg))
					h.sentBytes.Add(uint64(sendBytes))
				}
				topicMessageMap = make(map[string][]Record)
			}
		}
	}
}

func (h *HTTPMessageQueue) Close() error {
	h.closing.Store(true)
	if h.groupInstance == nil {
		return fmt.Errorf("group instance is nil")
	}
	_, err := DeleteConsumerGroupInstance(h.baseUrl, h.groupName, h.groupInstance.InstanceID)
	h.messageQueueClosed.Store(true)
	close(h.messageQueue)
	h.incomingMessagesClosed.Store(true)
	close(h.incomingMessages)
	return err
}

func (h *HTTPMessageQueue) GetMessages() <-chan kafka.Message {
	if h.incomingMessagesClosed.Load() {
		return nil
	}
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
	ticker := time.NewTicker(FiveSeconds)
	defer ticker.Stop()

	for range ticker.C {
		if h.closing.Load() {
			zap.S().Infof("Closing topic refresher")
			return
		}
		topics, e, err := GetTopics(h.baseUrl)
		if err != nil || e != nil {
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
			continue
		}
		zap.S().Infof("Subscribing to new topics: %+v", newTopics)

		var subtopics = SubscribeTopics{
			Topics: newTopics,
		}
		if h.groupInstance == nil {
			zap.S().Debugf("group instance is nil")
			continue
		}

		topicErr, err := PostSubscribeToTopic(h.baseUrl, h.groupName, h.groupInstance.InstanceID, subtopics)
		if err != nil {
			zap.S().Warnf("Error subscribing to topic: %v", err)
		}
		if topicErr != nil {
			zap.S().Warnf("Error subscribing to topic (TE): %v", topicErr)
		}
		previousTopics = topicList
	}

	zap.S().Infof("Closing topic refresher")
}

func (h *HTTPMessageQueue) consume() {
	zap.S().Infof("Starting consumer loop")
	var messages *[]RecordEx
	var bodyError *ErrorBody
	var err error
	ticker := time.NewTicker(OneHundredMilliseconds)
	defer ticker.Stop()

	for range ticker.C {
		if h.closing.Load() {
			zap.S().Infof("Closing consumer loop")
			return
		}
		if h.baseUrl == "" || h.groupName == "" || h.groupInstance == nil {
			zap.S().Debugf("Not consuming, missing baseUrl, groupName or groupInstance: %s, %s, %+v", h.baseUrl, h.groupName, h.groupInstance)
			continue
		}
		messages, bodyError, err = GetMessages(h.baseUrl, h.groupName, h.groupInstance.InstanceID)
		if err != nil {
			zap.S().Debugf("Error getting messages: %v for (%s,%s,%s)", err, h.baseUrl, h.groupName, h.groupInstance.InstanceID)
			continue
		}
		if bodyError != nil {
			zap.S().Debugf("Error getting messages: %#v for (%s,%s,%s)", bodyError, h.baseUrl, h.groupName, h.groupInstance.InstanceID)
			continue
		}
		if messages == nil {
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
			var msgX kafka.Message
			msgX, err = HTTPToKafkaMessage(message)
			if err != nil {
				zap.S().Debugf("Error converting message: %v", err)
				continue
			}
			h.receivedBytes.Add(uint64(len(msgX.Value)))
			h.receivedBytes.Add(uint64(len(message.Key)))
			if !h.incomingMessagesClosed.Load() || h.incomingMessages != nil {
				h.incomingMessages <- msgX
			}
		}
		var partitions = Partitions{
			Partitions: partitionList,
		}

		if len(partitions.Partitions) == 0 {
			continue
		}
		if h.groupInstance == nil {
			zap.S().Debugf("group instance is nil")
			continue
		}
		_, bodyError, err = PostCommitOffsets(h.baseUrl, h.groupName, h.groupInstance.InstanceID, partitions)
		if err != nil {
			zap.S().Debugf("Error committing offsets: %v", err)
			continue
		}
		if bodyError != nil {
			zap.S().Debugf("Error committing offsets: %v", bodyError)
			continue
		}
	}
	zap.S().Infof("Consumer loop closed")
}

func (h *HTTPMessageQueue) GetQueueLength() int {
	if h.messageQueue == nil {
		return 0
	}
	if h.messageQueueClosed.Load() {
		return 0
	}
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
