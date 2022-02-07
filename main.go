package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-shovel/kafka"
	"github.com/Trendyol/kafka-shovel/services"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
)

type EnvConfig struct {
	Topics       map[string]bool `required:"true"`
	ErrorSuffix  string          `required:"true"`
	RetrySuffix  string          `required:"true"`
	Brokers      []string        `required:"true"`
	KafkaVersion string
	RetryCount   int
	GroupName    string `required:"true"`
	Duration     int    `default:"5"`
}

func main() {
	var config EnvConfig
	envconfig.MustProcess("", &config)

	shovels := getShovels(config)
	for _, shovel := range shovels {
		runKafkaShovelListener(config, shovel)
	}

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

func runKafkaShovelListener(conf EnvConfig, shovel services.Shovel) {
	notificationChannel := make(chan string)
	config := kafka.ConnectionParameters{
		ConsumerGroupID: conf.GroupName,
		Conf:            KafkaConfig(conf.KafkaVersion, "kafkaShovelClient"),
		Brokers:         conf.Brokers,
		Topics:          []string{shovel.From},
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}

	service := services.NewService(producer, shovel)
	handler := services.NewEventHandler(service, notificationChannel)
	consumer.Subscribe(handler)

	go func() {
		for {
			select {
			case <-notificationChannel:
				consumer.Stop()
				<-time.Tick(time.Duration(conf.Duration) * time.Minute)
				consumer.Start()
			}
		}
	}()

	fmt.Printf("%v listener is starting", shovel.From)
	return
}

func getShovels(conf EnvConfig) (result []services.Shovel) {
	config := sarama.NewConfig()
	v, err := sarama.ParseKafkaVersion(conf.KafkaVersion)
	if err != nil {
		panic(err)
	}
	config.Version = v
	client, err := sarama.NewClient(conf.Brokers, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		for topicName, isInfinite := range conf.Topics {
			if !strings.Contains(topic, topicName) || !strings.Contains(topic, conf.ErrorSuffix) {
				continue
			}

			retryTopic := strings.ReplaceAll(topic, conf.ErrorSuffix, conf.RetrySuffix)
			result = append(result, services.Shovel{
				From:            topic,
				To:              retryTopic,
				IsInfiniteRetry: isInfinite,
				RetryCount:      conf.RetryCount,
			})
		}
	}
	return
}

func KafkaConfig(version, clientId string) *sarama.Config {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic(err)
	}
	//consumer
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true
	config.ClientID = clientId

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 2048 * 1024
	//producer
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 5 * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.DialTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Producer.MaxMessageBytes = 2000000

	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 5 * time.Second

	return config
}

func healthCheck(c *gin.Context) {
	c.JSON(200, "Healthy")
}
