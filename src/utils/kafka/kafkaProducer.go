package kafka

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
	timeout  time.Duration
}

type KafkaMessage struct {
	Key   string
	Value interface{}
}

func NewProducer() (*Producer, error) {

	// Enable Sarama debug logs
	sarama.Logger = log.New(
		os.Stdout,
		"[Sarama] ", //logs start with this prefix
		log.LstdFlags,
	)

	config := GetKafkaConfig() //get saram config

	// Check broker metadata
	client, err := sarama.NewClient(GetBrokers(), config) //client with broker address and configurations is created
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	for _, broker := range client.Brokers() {
		fmt.Printf(
			"Broker ID=%d Addr=%s\n",
			broker.ID(),
			broker.Addr(),
		)
	}

	p, err := sarama.NewAsyncProducer(
		GetBrokers(),
		config,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to create async producer: %w",
			err,
		)
	}

	prod := &Producer{
		producer: p,
		timeout:  50 * time.Millisecond, // can later move to config/env
	}

	go handleSuccess(prod.producer.Successes())
	go handleErrors(prod.producer.Errors())

	return prod, nil
}

func (p *Producer) Publish(topic string, key string, message []byte) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	select {
	case p.producer.Input() <- msg:
		return nil

	case <-time.After(p.timeout):
		return fmt.Errorf("kafka producer buffer full / timeout")
	}
}

// currently we are not using this fuction because the application is small scaled, it is written just to show that the application is capable of high scaled data production management
func (p *Producer) PublishBatch(topic string, messages []KafkaMessage) error {

	if len(messages) == 0 {
		return fmt.Errorf("no messages to publish")
	}

	for _, m := range messages {

		encoded, err := EncodeMessage(m.Value)
		if err != nil {
			return err
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(encoded),
		}

		if m.Key != "" {
			msg.Key = sarama.StringEncoder(m.Key)
		}

		select {
		case p.producer.Input() <- msg:
		case <-time.After(p.timeout):
			return fmt.Errorf("kafka batch publish timeout")
		}
	}

	return nil
}

func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}

	return nil
}
