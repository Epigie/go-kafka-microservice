package main

import (
	"encoding/json"
	"log"
	"net/http"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9094").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("important").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

type Message struct {
	ID      uint   `json:"id"`
	Content string `json:"content"`
	Status  string `json:"status"`
}

func main() {
	app := gin.Default()

	app.POST("/produce", func(c *gin.Context) {
		var message Message
		if err := c.ShouldBindJSON(&message); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		PushToKafka(message)
		c.JSON(http.StatusOK, gin.H{"message": "Message sent to Kafka"})
	})
	app.Run(":3000")

}

func PushToKafka(message Message) {

	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = *maxRetry
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(*brokerList, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()
	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Value: sarama.ByteEncoder(func() []byte {
			b, err := json.Marshal(message)
			if err != nil {
				log.Panic(err)
			}
			return b
		}()),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)
}
