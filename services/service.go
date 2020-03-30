package services

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"strconv"
)

type service struct {
	producer sarama.SyncProducer
	to       string
}

const RetryKey = "RetryCount"

type Service interface {
	OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error
}

func NewService(producer sarama.SyncProducer, to string) Service {
	return &service{
		to:       to,
		producer: producer,
	}
}

func (s *service) OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error {
	retryCount := getRetryCountFromHeader(message)
	if retryCount > 1 {
		fmt.Println("Message is ignored , message:", string(message.Value))
		return nil
	}

	if retryCount == 0 {
		message.Headers = append(message.Headers, &sarama.RecordHeader{
			Key:   []byte(RetryKey),
			Value: []byte("1"),
		})
	}

	headers := make([]sarama.RecordHeader, 0)
	for _, header := range message.Headers {
		headers = append(headers, *header)
	}
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   s.to,
		Value:   sarama.StringEncoder(message.Value),
		Headers: headers,
	})

	return err
}

func getRetryCountFromHeader(message *sarama.ConsumerMessage) int {
	for _, header := range message.Headers {
		if string(header.Key) == RetryKey {
			return getInt(header.Value)
		}
	}
	return 0
}

func getInt(s []byte) int {
	retryCountString := string(s)
	retryCount, _ := strconv.Atoi(retryCountString)
	return retryCount
}
