package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"order_management_system/src/models"
	"order_management_system/src/utils/configs"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// DLQProcessorConfig controls what the DLQ processor does with each dead letter.
type DLQProcessorConfig struct {
	// EnableReplay, when true, republishes dead letters back to the original topic
	// after the operator has had a chance to inspect them. Off by default because
	// replaying without fixing the root cause just loops messages back to the DLQ.
	EnableReplay bool

	// AlertThreshold is the number of DLQ messages that triggers a high-severity alert.
	AlertThreshold int
}

func DefaultDLQProcessorConfig() DLQProcessorConfig {
	return DLQProcessorConfig{
		EnableReplay:   false,
		AlertThreshold: 10,
	}
}

// DLQProcessor consumes from the DLQ topic and takes action on each dead letter:
// structured logging (for Datadog / ELK / CloudWatch), optional replay, optional alerting.
type DLQProcessor struct {
	producer  *Producer
	config    DLQProcessorConfig
	logger    *logrus.Logger
	msgCount  int // in-memory counter for alert threshold; replace with metrics in prod
}

func NewDLQProcessor(
	producer *Producer,
	config DLQProcessorConfig,
	logger *logrus.Logger,
) *DLQProcessor {
	return &DLQProcessor{
		producer: producer,
		config:   config,
		logger:   logger,
	}
}

// StartDLQProcessor launches the consumer loop for the DLQ topic.
// It runs in its own goroutine and respects ctx cancellation.
func StartDLQProcessor(
	ctx context.Context,
	baseTopic string,
	processor *DLQProcessor,
	logger *logrus.Logger,
) {
	dlqTopic := DLQTopicName(baseTopic)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	saramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	saramaConfig.Net.DialTimeout = 10 * time.Second
	saramaConfig.Net.ReadTimeout = 10 * time.Second
	saramaConfig.Net.WriteTimeout = 10 * time.Second
	saramaConfig.Metadata.Retry.Max = 5
	saramaConfig.Metadata.Retry.Backoff = 2 * time.Second
	saramaConfig.Version = sarama.V3_6_0_0

	cfg, err := configs.Get("application")
	if err != nil {
		panic(err)
	}

	baseGroupID := cfg.GetString("kafka.consumer_group")
	if baseGroupID == "" {
		baseGroupID = "order-consumer-group-v1"
	}
	groupID := fmt.Sprintf("%s-dlq-processor", baseGroupID)

	group, err := sarama.NewConsumerGroup(GetBrokers(), groupID, saramaConfig)
	if err != nil {
		panic(fmt.Sprintf("failed to create DLQ processor consumer group: %v", err))
	}
	defer group.Close()

	handler := &dlqConsumerHandler{processor: processor, logger: logger}

	logger.WithField("dlq_topic", dlqTopic).Info("DLQ processor started")

	for {
		if err := group.Consume(ctx, []string{dlqTopic}, handler); err != nil {
			logger.WithError(err).Error("DLQ processor error, retrying in 5s")
			time.Sleep(5 * time.Second)
		}
		if ctx.Err() != nil {
			logger.Info("context cancelled, stopping DLQ processor")
			return
		}
	}
}

// dlqConsumerHandler bridges sarama's ConsumerGroupHandler to DLQProcessor.
type dlqConsumerHandler struct {
	processor *DLQProcessor
	logger    *logrus.Logger
}

func (h *dlqConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *dlqConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *dlqConsumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		h.processor.handle(session, msg)
	}
	return nil
}

// handle processes one dead letter: extract metadata, log it, optionally replay.
func (p *DLQProcessor) handle(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	meta := extractMetadata(msg)

	p.msgCount++

	// Structured log — ingest into Datadog / ELK / CloudWatch directly.
	p.logger.WithFields(logrus.Fields{
		"dlq_topic":          msg.Topic,
		"original_topic":     meta.OriginalTopic,
		"original_offset":    meta.OriginalOffset,
		"original_partition": meta.OriginalPartition,
		"retry_count":        meta.RetryCount,
		"error_message":      meta.ErrorMessage,
		"error_type":         meta.ErrorType,
		"failed_at":          meta.FailedAt,
		"consumer_group":     meta.ConsumerGroup,
		"payload_size_bytes": len(msg.Value),
	}).Error("DEAD_LETTER: message could not be processed after all retries")

	// Alert if threshold is crossed.
	if p.msgCount >= p.config.AlertThreshold {
		p.logger.WithFields(logrus.Fields{
			"count":     p.msgCount,
			"threshold": p.config.AlertThreshold,
			"topic":     meta.OriginalTopic,
		}).Error("DLQ_ALERT: DLQ message count exceeded threshold — manual intervention required")
		// In production: fire a PagerDuty / Slack / SNS alert here instead of (or in addition to) logging.
		p.msgCount = 0 // reset so the alert fires again after the next threshold crossing
	}

	// Optional replay: republish to the original topic.
	if p.config.EnableReplay && meta.OriginalTopic != "" {
		if err := p.replay(meta.OriginalTopic, msg); err != nil {
			p.logger.WithError(err).Error("DLQ replay failed")
		} else {
			p.logger.WithField("original_topic", meta.OriginalTopic).Info("DLQ message replayed successfully")
		}
	}

	session.MarkMessage(msg, "")
}

// replay publishes the dead letter's payload back to its original topic.
// Headers are stripped so it enters the pipeline fresh (retry count resets to 0).
func (p *DLQProcessor) replay(originalTopic string, msg *sarama.ConsumerMessage) error {
	prod := &sarama.ProducerMessage{
		Topic: originalTopic,
		Value: sarama.ByteEncoder(msg.Value),
	}
	if len(msg.Key) > 0 {
		prod.Key = sarama.ByteEncoder(msg.Key)
	}

	select {
	case p.producer.producer.Input() <- prod:
		return nil
	case <-time.After(p.producer.timeout):
		return fmt.Errorf("replay publish timeout for topic %s", originalTopic)
	}
}

// extractMetadata reads the DLQ headers back into a DLQMetadata struct.
func extractMetadata(msg *sarama.ConsumerMessage) models.DLQMetadata {
	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[string(h.Key)] = string(h.Value)
	}

	retryCount, _ := strconv.Atoi(headers[models.HeaderRetryCount])
	originalOffset, _ := strconv.ParseInt(headers[models.HeaderOriginalOffset], 10, 64)
	originalPartition64, _ := strconv.ParseInt(headers[models.HeaderOriginalPartition], 10, 32)
	failedAt, _ := time.Parse(time.RFC3339, headers[models.HeaderFailedAt])

	return models.DLQMetadata{
		OriginalTopic:     headers[models.HeaderOriginalTopic],
		OriginalOffset:    originalOffset,
		OriginalPartition: int32(originalPartition64),
		RetryCount:        retryCount,
		ErrorMessage:      headers[models.HeaderErrorMessage],
		ErrorType:         headers[models.HeaderErrorType],
		FailedAt:          failedAt,
		ConsumerGroup:     headers[models.HeaderConsumerGroup],
	}
}