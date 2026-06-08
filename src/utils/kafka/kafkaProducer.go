package kafka

import (
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.AsyncProducer
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
	client, err := sarama.NewClient(Brokers, config) //client with broker address and configurations is created
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
		Brokers,
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
	}

	go prod.handleSuccess()
	go prod.handleErrors()

	return prod, nil
}

func (p *Producer) Publish(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{ //ProducerMessage is the collection of elements passed to the Producer in order to send a message.
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	fmt.Printf("Topic: %s\n", topic)
	fmt.Printf("Message: %s\n", string(message))

	p.producer.Input() <- msg

	return nil
}

func (p *Producer) handleSuccess() {
	for msg := range p.producer.Successes() {

		fmt.Printf(
			"Topic=%s Partition=%d Offset=%d\n",
			msg.Topic,
			msg.Partition,
			msg.Offset,
		)
	}
}

func (p *Producer) handleErrors() {
	for err := range p.producer.Errors() {

		fmt.Printf(
			"Error: %v\n",
			err.Err,
		)

		if err.Msg != nil {

			fmt.Printf(
				"Topic: %s\n",
				err.Msg.Topic,
			)

			fmt.Printf(
				"Partition: %d\n",
				err.Msg.Partition,
			)
		}
	}
}

func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}

	return nil
}
