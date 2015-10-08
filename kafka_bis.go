package main

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/jeffail/gabs"
	"github.com/olivere/elastic"
	"sync"
	"time"
)

func forwardMessages(wg sync.WaitGroup, kafkaClient sarama.Client, topic string) error {
	defer wg.Done()
	partition := int32(0)

	offset, err := kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(kafkaClient)
	if err != nil {
		return err
	}

	consumePartition, err := consumer.ConsumePartition(topic, 0, offset)
	if err != nil {
		return err
	}

	elasticClient, err := elastic.NewClient(elastic.SetURL("http://192.168.6.27:9200"))
	if err != nil {
		return err
	}

	for m := range consumePartition.Messages() {
		jsonParsed, err := gabs.ParseJSON(m.Value)
		if err != nil {
			log.Warnf("error parsing message: %s", err)
			continue
		}

		jsonParsed.Set(time.Now(), "timestamp")

		jsonParsed.Delete("picture")
		_, err = elasticClient.Index().
			Index("logstash-kafka").
			Type(topic).
			BodyJson(jsonParsed.String()).
			Do()
		if err != nil {
			log.Warnf("error sending message to elasticsearch: %s", err)
		}
		log.Infof("Message on topic '%s': %s", topic, string(jsonParsed.String()))
	}

	return nil
}

func main() {

	config := sarama.NewConfig()

	client, err := sarama.NewClient([]string{"192.168.7.248:9092"}, config)
	if err != nil {
		log.Fatal("Error connectiong to kafka: ", err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go forwardMessages(wg, client, "heartbeats")
	wg.Add(1)
	go forwardMessages(wg, client, "pictures")
	wg.Add(1)
	go forwardMessages(wg, client, "sales")

	wg.Wait()
}
