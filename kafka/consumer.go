package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type Consumer interface {
	Subscribe(handler EventHandler)
	Start()
	Stop()
}

type EventHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

type kafkaConsumer struct {
	topic         []string
	consumerGroup sarama.ConsumerGroup
}

func NewConsumer(connectionParams ConnectionParameters) (Consumer, error) {
	cg, err := sarama.NewConsumerGroup(connectionParams.Brokers, connectionParams.ConsumerGroupID, connectionParams.Conf)
	if err != nil {
		return nil, err
	}

	return &kafkaConsumer{
		topic:         connectionParams.Topics,
		consumerGroup: cg,
	}, nil
}

func (c *kafkaConsumer) Subscribe(handler EventHandler) {
	ctx := context.Background()

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, c.topic, handler); err != nil {
				fmt.Println("Error from consumer group : ", err.Error())
				return
			}

			if ctx.Err() != nil {
				fmt.Println("Error from consumer group : ", ctx.Err())
				return
			}
		}
	}()
	go func() {
		for err := range c.consumerGroup.Errors() {
			fmt.Println("Error from consumer group : ", err.Error())
		}
	}()
}

func (c *kafkaConsumer) Start() {
	c.consumerGroup.ResumeAll()
}

func (c *kafkaConsumer) Stop() {
	c.consumerGroup.PauseAll()
}
