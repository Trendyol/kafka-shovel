package services

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-shovel/kafka"
)

type Interceptor func(ctx context.Context, message *sarama.ConsumerMessage) context.Context

type eventHandler struct {
	Service
}

func NewEventHandler(service Service) kafka.EventHandler {
	return &eventHandler{
		service,
	}
}

func (e *eventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (e *eventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Println("Received messages", string(message.Value))
		err := e.Service.OperateEvent(context.Background(), message)
		if err != nil {
			fmt.Println("Error executing err: ", err)
		}

		session.MarkMessage(message, "")
	}

	return nil
}
