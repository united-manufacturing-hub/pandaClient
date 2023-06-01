# pandaClient

This go package provides a wrapper around our [sarama kafka client](https://github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper).
It provides a single interface with automatic fallback to an HTTP based alternative (https://redpanda.com/blog/pandaproxy).

It makes no stability or compatibility guarantees. It is provided as-is.
