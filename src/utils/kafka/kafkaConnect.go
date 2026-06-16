package kafka

import (
	"order_management_system/src/utils/configs"

	"github.com/IBM/sarama"
)

func GetBrokers() []string {

	cfg, err := configs.Get("application")
	if err != nil {
		panic(err)
	}

	brokers := cfg.GetStringSlice("kafka.brokers")

	if len(brokers) == 0 {
		return []string{
			"localhost:9092",
		}
	}

	return brokers
}

func GetKafkaConfig() *sarama.Config { //config is used to pass multiple configuration properties to sarama's constructor

	config := sarama.NewConfig()

	config.Version = sarama.V3_6_0_0

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Partitioner = sarama.NewRandomPartitioner //returns partitioner that selects partition randomly

	config.Metadata.Full = true

	cfg, err := configs.Get("application")
	if err != nil {
		panic(err)
	}

	compression := cfg.GetString("kafka.compression")

	// pass to helper
	applyCompression(config, compression)

	return config
}
