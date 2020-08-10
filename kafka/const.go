package kafka

import "github.com/Shopify/sarama"

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         []string
	Topics          []string
	ConsumerGroupID string
}
