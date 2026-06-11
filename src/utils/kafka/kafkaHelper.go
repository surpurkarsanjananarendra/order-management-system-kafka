package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
)

func applyCompression(config *sarama.Config, compression string) {

	switch compression {
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}
}

func EncodeMessage(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func handleSuccess(successes <-chan *sarama.ProducerMessage) {
	for msg := range successes {
		fmt.Printf(
			"Topic=%s Partition=%d Offset=%d\n",
			msg.Topic,
			msg.Partition,
			msg.Offset,
		)
	}
}

func handleErrors(errors <-chan *sarama.ProducerError) {
	for err := range errors {
		fmt.Printf("Error: %v\n", err.Err)
	}
}
