package kafka

import "github.com/Shopify/sarama"

const (
	Stop = "Stop"
)

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         []string
	Topics          []string
	ConsumerGroupID string
}
