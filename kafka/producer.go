package kafka

import (
	"github.com/Shopify/sarama"
)

func NewProducer(connectionParams ConnectionParameters) (sarama.SyncProducer, error) {
	syncProducer, err := sarama.NewSyncProducer(connectionParams.Brokers, connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return syncProducer, err
}
