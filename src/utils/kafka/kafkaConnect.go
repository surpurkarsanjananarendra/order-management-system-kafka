package kafka

import (
	"os"
	"strings"

	"github.com/IBM/sarama"
)

func GetBrokers() []string {

	brokers := os.Getenv("KAFKA_BROKERS")

	if brokers == "" {
		return []string{
			"localhost:9092",
		}
	}

	return strings.Split(brokers, ",")
}

func GetKafkaConfig() *sarama.Config { //config is used to pass multiple configuration properties to sarama's constructor

	config := sarama.NewConfig()

	config.Version = sarama.V3_6_0_0

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Partitioner = sarama.NewRandomPartitioner //returns partitioner that selects partition randomly

	config.Metadata.Full = true

	compression := os.Getenv("KAFKA_COMPRESSION")

	// pass to helper
	applyCompression(config, compression)

	return config
}
