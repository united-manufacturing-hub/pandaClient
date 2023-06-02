package pandaClient

import (
	"fmt"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"github.com/united-manufacturing-hub/pandaClient/pkg/pandaClient/pandaproxy"
	"go.uber.org/zap"
	"time"
)

type HTTPClientOptions struct {
	BaseURL string
}

type PandaClient struct {
	httpOpts         *HTTPClientOptions
	kafkaClient      *kafka.Client
	incomingMessages chan kafka.Message
	kafkaOpts        kafka.NewClientOptions
	httpMessageQueue pandaproxy.HTTPMessageQueue
	canUseKafka      bool
	canUseHTTP       bool
}

func New(options kafka.NewClientOptions, httpOpts *HTTPClientOptions) *PandaClient {
	return &PandaClient{
		kafkaOpts:        options,
		httpOpts:         httpOpts,
		incomingMessages: make(chan kafka.Message, 1000),
	}
}

func (p *PandaClient) Connect() (kafkaConnected, httpConnected bool, kafkaConnectError error, httpConnectError error) {
	// Try to connect to kafka
	// If it fails, set canUseKafka to false

	var err error
	p.kafkaClient, err = kafka.NewKafkaClient(&p.kafkaOpts)
	if err != nil {
		kafkaConnectError = err
		p.canUseKafka = false
	} else {
		p.canUseKafka = true
	}

	if p.httpOpts != nil {
		// Try to connect to http
		// If it fails, set canUseHTTP to false

		var brokers *pandaproxy.Brokers
		var errBody *pandaproxy.ErrorBody
		brokers, errBody, err = pandaproxy.GetBrokers(p.httpOpts.BaseURL)
		if err != nil {
			httpConnectError = err
			p.canUseHTTP = false
		} else if errBody != nil {
			httpConnectError = fmt.Errorf("HTTP connection failed: %v", errBody)
			p.canUseHTTP = false
		} else if brokers == nil {
			httpConnectError = fmt.Errorf("HTTP connection failed (brokers nil): %v", brokers)
			p.canUseHTTP = false
		} else {
			p.canUseHTTP = true
		}
	}
	if p.canUseHTTP {
		p.httpMessageQueue = pandaproxy.New(p.httpOpts.BaseURL)
		go p.httpMessageQueue.StartMessageSender()
		err = p.httpMessageQueue.StartSubscriber(fmt.Sprintf("%s-http", p.kafkaOpts.ClientID), fmt.Sprintf("%s-http", p.kafkaOpts.ConsumerName), p.kafkaOpts.ListenTopicRegex)
		if err != nil {
			httpConnectError = err
			p.canUseHTTP = false
		}
	}
	go p.readMessages()
	return p.canUseKafka, p.canUseHTTP, kafkaConnectError, httpConnectError
}

func (p *PandaClient) Close() (errK error, errH error) {
	if p.kafkaClient != nil {
		errK = p.kafkaClient.Close()
	}
	if p.canUseHTTP {
		errH = p.httpMessageQueue.Close()
	}
	return errK, errH
}

func (p *PandaClient) EnqueueMessage(message kafka.Message) error {
	if p.canUseKafka {
		return p.kafkaClient.EnqueueMessage(message)
	} else if p.canUseHTTP {
		return p.httpMessageQueue.EnqueueMessage(message)
	}
	return fmt.Errorf("cannot enqueue message: no connection to kafka or http")
}

func (p *PandaClient) GetMessages() <-chan kafka.Message {
	return p.incomingMessages
}

func (p *PandaClient) readMessages() {
	for !p.Closed() {
		if len(p.incomingMessages) >= cap(p.incomingMessages) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if p.canUseKafka {
			var chanX = p.kafkaClient.GetMessages()
			if chanX == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			select {
			case message := <-chanX:
				p.incomingMessages <- message
			case <-time.After(100 * time.Millisecond):
				continue
			}
		} else if p.canUseHTTP {
			var chanX = p.httpMessageQueue.GetMessages()
			if chanX == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			select {
			case message := <-chanX:
				p.incomingMessages <- message
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
	}
}

func (p *PandaClient) GetQueueLength() int {
	if p.canUseKafka {
		return p.kafkaClient.GetQueueLength()
	} else if p.canUseHTTP {
		return p.httpMessageQueue.GetQueueLength()
	}
	return 0
}

func (p *PandaClient) Ready() bool {
	if p.canUseKafka {
		return p.kafkaClient.Ready()
	} else if p.canUseHTTP {
		return p.httpMessageQueue.Ready()
	}
	return false
}

func (p *PandaClient) Closed() bool {
	if p.canUseKafka {
		zap.S().Debugf("kafkaClient.Closed() = %v", p.kafkaClient.Closed())
		return p.kafkaClient.Closed()
	} else if p.canUseHTTP {
		zap.S().Debugf("httpMessageQueue.Closed() = %v", p.httpMessageQueue.Closed())
		return p.httpMessageQueue.Closed()
	}
	return true
}

func (p *PandaClient) GetStats() (sent uint64, received uint64, sendBytesA uint64, receivedBytesA uint64) {
	if p.canUseKafka {
		return kafka.GetKafkaStats()
	} else if p.canUseHTTP {
		return p.httpMessageQueue.GetStats()
	}
	return 0, 0, 0, 0
}
