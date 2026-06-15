package kafka

import (
	"order_management_system/src/config"
	"strings"

	"github.com/IBM/sarama"
)

func GetBrokers() []string {

	cfg, err := config.Get(".env")
	if err != nil {
		panic(err)
	}

	brokers := cfg.GetString("KAFKA_BROKERS")

	if brokers == "" {
		return []string{
			"localhost:9092",
		}
	}

	return strings.Split(brokers, ",")
}

func GetKafkaConfig() *sarama.Config { //config is used to pass multiple configuration properties to sarama's constructor

	configs := sarama.NewConfig()

	configs.Version = sarama.V3_6_0_0

	configs.Producer.Return.Successes = true
	configs.Producer.Return.Errors = true

	configs.Producer.RequiredAcks = sarama.WaitForAll

	configs.Producer.Partitioner = sarama.NewRandomPartitioner //returns partitioner that selects partition randomly

	configs.Metadata.Full = true

	cfg, err := config.Get(".env")
	if err != nil {
		panic(err)
	}

	compression := cfg.GetString("KAFKA_COMPRESSION")

	// pass to helper
	applyCompression(configs, compression)

	return configs
}
