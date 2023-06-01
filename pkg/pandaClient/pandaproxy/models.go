package pandaproxy

type ErrorBody struct {
	Message string `json:"message"`
	Code    int64  `json:"code"`
}

type Brokers struct {
	Brokers []int64 `json:"brokers"`
}

type SubscribeTopics struct {
	Topics Topics `json:"topics"`
}

type Topics []string

type Messages struct {
	Records []Record `json:"records"`
}

type Record struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int64  `json:"partition"`
}

type MessageOffsets struct {
	Offsets []Offset `json:"offsets"`
}

type Offset struct {
	Partition int64 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type ConsumerGroup struct {
	Format                   string `json:"format"`
	Name                     string `json:"name"`
	AutoOffsetReset          string `json:"auto.offset.reset"`
	AutoCommitEnable         string `json:"auto.commit.enable"`
	FetchMinBytes            string `json:"fetch.min.bytes"`
	ConsumerRequestTimeoutMS string `json:"consumer.request.timeout.ms"`
}

type ConsumerGroupInstance struct {
	InstanceID string `json:"instance_id"`
	BaseURI    string `json:"base_uri"`
}

type MessagesEx struct {
	Records []RecordEx `json:"records"`
}

type RecordEx struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int64  `json:"partition"`
	Offset    int64  `json:"offset"`
}
type Partitions struct {
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	Topic     string `json:"topic"`
	Partition int64  `json:"partition"`
	Offset    int64  `json:"offset"`
}
