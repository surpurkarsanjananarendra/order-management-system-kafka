package kafka

import (
	"fmt"
	"order_management_system/src/models"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// RetryConfig defines the retry ladder for a topic.
// Each entry in Delays is one retry hop; after all hops are exhausted
// the message is published to the DLQ.
type RetryConfig struct {
	// Delays holds the per-hop delay. len(Delays) == number of retry topics.
	// e.g. []time.Duration{5*time.Second, 30*time.Second, 120*time.Second}
	Delays []time.Duration
}

// TopicNames returns the retry topic names derived from the base topic.
func (rc *RetryConfig) TopicNames(baseTopic string) []string {
	names := make([]string, len(rc.Delays))
	for i := range rc.Delays {
		names[i] = fmt.Sprintf("%s.retry-%d", baseTopic, i+1)
	}
	return names
}

// DLQTopicName returns the dead letter topic name for a base topic.
func DLQTopicName(baseTopic string) string {
	return baseTopic + ".dlq"
}

// DefaultRetryConfig is a sensible production default:
// retry 1 after 5s, retry 2 after 30s, retry 3 after 120s, then DLQ.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		Delays: []time.Duration{
			5 * time.Second,
			30 * time.Second,
			120 * time.Second,
		},
	}
}

// DLQRouter is responsible for publishing failed messages to either the next
// retry topic or the final DLQ. It is safe for concurrent use.
type DLQRouter struct {
	producer      *Producer
	retryConfig   RetryConfig
	baseTopic     string
	consumerGroup string
	logger        *logrus.Logger
}

func NewDLQRouter(producer *Producer, baseTopic string, consumerGroup string, retryConfig RetryConfig, logger *logrus.Logger) *DLQRouter {
	return &DLQRouter{
		producer:      producer,
		retryConfig:   retryConfig,
		baseTopic:     baseTopic,
		consumerGroup: consumerGroup,
		logger:        logger,
	}
}

// Route decides whether to send msg to the next retry topic or the DLQ
// based on the current retry count stored in the message headers.
func (d *DLQRouter) Route(msg *sarama.ConsumerMessage, err error, errType string) error {
	retryCount := d.currentRetryCount(msg)
	nextCount := retryCount + 1
	maxRetries := len(d.retryConfig.Delays)

	var destination string
	if retryCount >= maxRetries {
		destination = DLQTopicName(d.baseTopic)
		d.logger.WithFields(logrus.Fields{
			"topic":       msg.Topic,
			"partition":   msg.Partition,
			"offset":      msg.Offset,
			"retry_count": retryCount,
			"error":       err.Error(),
		}).Error("message exhausted all retries, routing to DLQ")
	} else {
		retryTopics := d.retryConfig.TopicNames(d.baseTopic)
		destination = retryTopics[retryCount] // index 0 = retry-1, 1 = retry-2, ...
		d.logger.WithFields(logrus.Fields{
			"topic":       msg.Topic,
			"partition":   msg.Partition,
			"offset":      msg.Offset,
			"retry_count": retryCount,
			"destination": destination,
			"error":       err.Error(),
		}).Warn("routing message to retry topic")
	}

	headers := d.buildHeaders(msg, nextCount, maxRetries, err, errType)
	return d.publish(destination, msg.Key, msg.Value, headers)
}

// buildHeaders assembles the sarama headers that travel with the routed message.
func (d *DLQRouter) buildHeaders(original *sarama.ConsumerMessage, retryCount int, maxRetries int, err error, errType string) []sarama.RecordHeader {
	// Carry forward any existing headers from previous hops first,
	// then overwrite the mutable ones.
	existing := make(map[string][]byte)
	for _, h := range original.Headers {
		existing[string(h.Key)] = h.Value
	}

	set := func(key, val string) sarama.RecordHeader {
		return sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(val),
		}
	}

	// Preserve original-topic only on the very first failure.
	originalTopic := string(existing[models.HeaderOriginalTopic])
	if originalTopic == "" {
		originalTopic = original.Topic
	}

	originalOffset := string(existing[models.HeaderOriginalOffset])
	if originalOffset == "" {
		originalOffset = strconv.FormatInt(original.Offset, 10)
	}

	originalPartition := string(existing[models.HeaderOriginalPartition])
	if originalPartition == "" {
		originalPartition = strconv.FormatInt(int64(original.Partition), 10)
	}

	return []sarama.RecordHeader{
		set(models.HeaderOriginalTopic, originalTopic),
		set(models.HeaderOriginalOffset, originalOffset),
		set(models.HeaderOriginalPartition, originalPartition),
		set(models.HeaderRetryCount, strconv.Itoa(retryCount)),
		set(models.HeaderErrorMessage, err.Error()),
		set(models.HeaderErrorType, errType),
		set(models.HeaderFailedAt, time.Now().UTC().Format(time.RFC3339)),
		set(models.HeaderConsumerGroup, d.consumerGroup),
	}
}

// publish sends the message to the target topic with the given headers.
func (d *DLQRouter) publish(topic string, key, value []byte, headers []sarama.RecordHeader) error {
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}
	if len(key) > 0 {
		msg.Key = sarama.ByteEncoder(key)
	}

	select {
	case d.producer.producer.Input() <- msg:
		return nil
	case <-time.After(d.producer.timeout):
		return fmt.Errorf("DLQ publish timeout for topic %s", topic)
	}
}

// currentRetryCount reads the retry count from the message headers.
// Returns 0 if the header is absent (i.e. this is the first failure).
func (d *DLQRouter) currentRetryCount(msg *sarama.ConsumerMessage) int {
	for _, h := range msg.Headers {
		if string(h.Key) == models.HeaderRetryCount {
			n, err := strconv.Atoi(string(h.Value))
			if err == nil {
				return n
			}
		}
	}
	return 0
}
