package kafka

import "github.com/IBM/sarama"

var Brokers = []string{
	"192.168.3.163:19092",
}

func GetKafkaConfig() *sarama.Config { //config is used to pass multiple configuration properties to sarama's constructor

	config := sarama.NewConfig()

	config.Version = sarama.V3_6_0_0

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Partitioner = sarama.NewRandomPartitioner//returns partitioner that selects partition randomly

	config.Metadata.Full = true

	return config
}
