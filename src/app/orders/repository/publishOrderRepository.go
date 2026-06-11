package repository

import (
	genericModels "order_management_system/src/models"
	"order_management_system/src/utils/kafka"
	"orders/models"
	"time"
)

type KafkaRepository struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaRepository(topic string, producer *kafka.Producer) *KafkaRepository {

	return &KafkaRepository{
		producer: producer,
		topic:    topic,
	}
}

func (k *KafkaRepository) PublishOrder(request models.BFFPublishOrderRequest) error {

	event := genericModels.OrderEvent{
		OrderID:     request.OrderID,
		CustomerID:  request.CustomerID,
		ProductName: request.ProductName,
		Quantity:    request.Quantity,
		Price:       request.Price,
		CreatedAt:   time.Now(),
	}

	payload, err := kafka.EncodeMessage(event)
	if err != nil {
		return err
	}
	return k.producer.Publish(k.topic, request.OrderID, payload)
}
