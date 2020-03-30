package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-shovel/services"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/gin-gonic/gin"
	"github.com/robfig/cron"
	"os"
	"strings"
	"time"
)

type Shovel struct {
	From string
	To   string
}

const Error = "ERROR"
const Retry = "RETRY"

func main() {
	c := cron.New()
	c.AddFunc("@every 15m", runAllShovels)
	c.Start()
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.GET("/_monitoring/ready", healthCheck)
	r.GET("/_monitoring/live", healthCheck)

	appPort := "8080"
	fmt.Println("[x] Kafka Shovel is running in " + appPort)

	if err := r.Run(":" + appPort); err != nil {
		fmt.Println(err)
		return
	}
}

func runAllShovels() {
	var channels []chan bool
	shovels := getShovels()
	for _, v := range shovels {
		channels = append(channels, runKafkaShovelListener(v))
	}

	go func() {
		<-time.Tick(3 * time.Minute)
		for _, v := range channels {
			close(v)
		}
	}()

}

func runKafkaShovelListener(shovel Shovel) chan bool {
	notificationChannel := make(chan bool)
	config := kafka_wrapper.ConnectionParameters{
		ConsumerGroupID: "kafkaShovel",
		ClientID:        "kafkaShovelClient",
		Brokers:         os.Getenv("BROKERS"),
		Version:         os.Getenv("KAFKA_VERSION"),
		Topic:           []string{shovel.From},
		FromBeginning:   true,
	}

	consumer, err := kafka_wrapper.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	producer, err := kafka_wrapper.NewProducer(config)
	if err != nil {
		panic(err)
	}

	service := services.NewService(producer, shovel.To)
	handler := services.NewEventHandler(service)
	consumer.Subscribe(handler)
	go func() {
		<-notificationChannel
		consumer.Unsubscribe()
	}()
	fmt.Printf("%v listener is starting", shovel.From)
	return notificationChannel
}

func getShovels() (result []Shovel) {
	config := sarama.NewConfig()
	v, err := sarama.ParseKafkaVersion(os.Getenv("KAFKA_VERSION"))
	if err != nil {
		panic(err)
	}
	config.Version = v
	client, err := sarama.NewClient(strings.Split(os.Getenv("BROKERS"), ","), config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		if strings.Contains(topic, Error) {
			retryTopic := strings.ReplaceAll(topic, Error, Retry)
			result = append(result, Shovel{
				From: topic,
				To:   retryTopic,
			})
		}
	}
	return
}

func healthCheck(c *gin.Context) {
	c.JSON(200, "Healthy")
}
