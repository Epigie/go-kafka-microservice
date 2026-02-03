package main

import (
	"encoding/json"
	"log"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/vsouza/go-kafka/example/database"
	"github.com/vsouza/go-kafka/example/models"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("broker:9092").Envar("KAFKA_BROKERS").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("database").Envar("KAFKA_TOPIC").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").Int32()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

func main() {
	kingpin.Parse()

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := *brokerList
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()

	// Get all partitions for the topic
	partitions, err := master.Partitions(*topic)
	if err != nil {
		log.Panic(err)
	}

	messageCount := int64(*messageCountStart) // can use atomic for thread safety

	for _, partition := range partitions {
		go func(p int32) {
			consumer, err := master.ConsumePartition(*topic, p, sarama.OffsetOldest)
			if err != nil {
				log.Panic(err)
			}
			defer consumer.Close()

			dbSvc := database.New()
			defer func() {
				if err := dbSvc.Close(); err != nil {
					log.Println("error closing db:", err)
				}
			}()

			for {
				select {
				case err := <-consumer.Errors():
					log.Println("Consumer error:", err)
				case msg := <-consumer.Messages():
					messageCount++
					log.Printf("Partition:%d Offset:%d Received message: key=%s value=%s\n",
						msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

					var m models.Message
					if err := json.Unmarshal(msg.Value, &m); err != nil {
						log.Println("Failed to unmarshal message:", err)
						continue
					}
					m.Status = "received"
					if err := database.DB.Create(&m).Error; err != nil {
						log.Println("Failed to save message:", err)
					} else {
						log.Println("Saved message ID:", m.ID)
					}
				}
			}
		}(partition)
	}

	// Block forever
	select {}
}

