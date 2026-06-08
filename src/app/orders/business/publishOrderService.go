package business

import (
	"orders/models"
	"orders/repository"
)

type PublishOrderService struct {
	kafkaRepo *repository.KafkaRepository
}

func NewPublishOrderService(kafkaRepo *repository.KafkaRepository) *PublishOrderService {
	return &PublishOrderService{
		kafkaRepo: kafkaRepo,
	}
}

func (s *PublishOrderService) PublishOrder(request models.BFFPublishOrderRequest) error {
	err := s.kafkaRepo.PublishOrder(request)

	if err != nil {
		return err
	}
	return nil
}
