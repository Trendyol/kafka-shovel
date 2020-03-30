package services

import (
	"fmt"
	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
)

type eventHandler struct {
	service Service
}

func NewEventHandler(service Service) kafka_wrapper.EventHandler {
	return &eventHandler{
		service: service,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (e *eventHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (e *eventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Println("Received messages", string(message.Value))
		err := e.service.OperateEvent(session.Context(), message)
		if err != nil {
			fmt.Println("Error executing err: ", err)
		}

		session.MarkMessage(message, "")
	}

	return nil
}
