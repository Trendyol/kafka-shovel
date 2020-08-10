package services

import (
	"context"
	"github.com/Shopify/sarama"
	"strconv"
)

type Shovel struct {
	From            string
	To              string
	IsInfiniteRetry bool
	RetryCount      int
}

type service struct {
	producer sarama.SyncProducer
	shovel   Shovel
}

const RetryKey = "RetryCount"

type Service interface {
	OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error
}

func NewService(producer sarama.SyncProducer, shovel Shovel) Service {
	return &service{
		shovel:   shovel,
		producer: producer,
	}
}

func (s *service) OperateEvent(ctx context.Context, message *sarama.ConsumerMessage) error {
	retryCount := getRetryCountFromHeader(message)
	if retryCount > s.shovel.RetryCount && !s.shovel.IsInfiniteRetry {
		return nil
	} else {
		retryCount++
		replaceRetryCount(message, retryCount)
	}
	headers := make([]sarama.RecordHeader, 0)
	for _, header := range message.Headers {
		headers = append(headers, *header)
	}
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   s.shovel.To,
		Value:   sarama.StringEncoder(message.Value),
		Headers: headers,
		Key:     sarama.StringEncoder(message.Key),
	})

	return err
}

func replaceRetryCount(message *sarama.ConsumerMessage, retryCount int) {
	isFound := false
	for _, header := range message.Headers {
		if string(header.Key) == RetryKey {
			isFound = true
			header.Value = []byte(strconv.Itoa(retryCount))
		}
	}
	if !isFound {
		message.Headers = append(message.Headers, &sarama.RecordHeader{
			Key:   []byte(RetryKey),
			Value: []byte(strconv.Itoa(retryCount)),
		})
	}
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
