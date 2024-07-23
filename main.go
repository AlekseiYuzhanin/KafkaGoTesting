package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer *kafka.Producer
	topic string
	deliverych chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer{

	return &OrderPlacer{
		producer: p,
		topic: topic,
		deliverych: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(ordertype string, size int) error {
	format := fmt.Sprintf("%s - %d", ordertype, size)
	payload := []byte(format)
	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value: payload},
		op.deliverych,
	)

	if err != nil {
		log.Fatal(err)
	}

	<-op.deliverych
	return nil
} 

func main() {
	topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers" : "localhost:9092",
		"client.id" : "foo",
		"acks" : "all",
	})

	if err != nil{
		fmt.Printf("Failed to create producer %s\n", err)
	}

	op := NewOrderPlacer(p, topic)

	for i := 0; i< 10000;i++{
		err := op.placeOrder("Market", i);if err != nil{
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}

	val := 7
	f := []int{0,1}
	for len(f) <= val{
		f = append(f, (len(f) - 1 + len(f) - 2)) 
	}
	fmt.Println(f[val])
	
}

