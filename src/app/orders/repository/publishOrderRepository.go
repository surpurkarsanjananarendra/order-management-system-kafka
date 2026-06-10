package repository

import (
	"encoding/json"
	"log"
	"order_management_system/src/utils/kafka"
	"orders/models"
	"time"
)

type KafkaRepository struct {
	producer *kafka.Producer
}

func NewKafkaRepository(producer *kafka.Producer) *KafkaRepository {

	return &KafkaRepository{
		producer: producer,
	}
}

func (k *KafkaRepository) PublishOrder(request models.BFFPublishOrderRequest) error {
	ordersBytes, _ := json.Marshal(request) //convert to json inorder to add a new field

	var order map[string]any
	if err := json.Unmarshal(ordersBytes, &order); err != nil { //convert to struct to have the updated request body
		log.Fatalf("Error unmarshaling: %v", err)
	}

	order["created_at"] = time.Now()

	encodedMessage, err := kafka.EncodeMessage(order) //convert it again to json so that kafka can process and store event

	if err != nil {
		return err
	}
	err = k.producer.Publish(
		"temp",         //topic name
		encodedMessage, //message to be stored
	)

	if err != nil {
		return err
	}

	return nil
}
