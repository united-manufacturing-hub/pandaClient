package pandaproxy

type ErrorBody struct {
	Message string `json:"message"`
	Code    int64  `json:"code"`
}

type Brokers struct {
	Brokers []int64 `json:"brokers"`
}
