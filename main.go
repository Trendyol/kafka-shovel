package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-shovel/kafka"
	"github.com/Trendyol/kafka-shovel/services"
	"github.com/gin-gonic/gin"
	"github.com/kelseyhightower/envconfig"
	"github.com/robfig/cron"
	"strings"
	"time"
)

type EnvConfig struct {
	Topics       map[string]bool
	ErrorPrefix  string
	RetryPrefix  string
	Brokers      []string
	KafkaVersion string
	RetryCount   int
	RunningTime  int
}

func main() {
	c := cron.New()
	runAllShovels()
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
	var config EnvConfig
	err := envconfig.Process("", &config)
	if err != nil {
		panic(err)
	}

	var channels []chan bool
	shovels := getShovels(config)
	for _, shovel := range shovels {
		channels = append(channels, runKafkaShovelListener(config, shovel))
	}

	go func() {
		<-time.Tick(time.Duration(config.RunningTime) * time.Minute)
		for _, v := range channels {
			close(v)
		}
	}()

}

func runKafkaShovelListener(conf EnvConfig, shovel services.Shovel) chan bool {
	notificationChannel := make(chan bool)
	config := kafka.ConnectionParameters{
		ConsumerGroupID: "kafkaShovel",
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
	handler := services.NewEventHandler(service)
	errChannel := consumer.Subscribe(handler)
	go func() {
		for e := range errChannel {
			fmt.Println(e)
			_ = producer.Close()
		}
	}()
	go func() {
		<-notificationChannel
		consumer.Unsubscribe()
	}()
	fmt.Printf("%v listener is starting", shovel.From)
	return notificationChannel
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
			if strings.Contains(topic, topicName) && strings.Contains(topic, conf.ErrorPrefix) {
				retryTopic := strings.ReplaceAll(topic, conf.ErrorPrefix, conf.RetryPrefix)
				result = append(result, services.Shovel{
					From:            topic,
					To:              retryTopic,
					IsInfiniteRetry: isInfinite,
					RetryCount:      conf.RetryCount,
				})
			}
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
