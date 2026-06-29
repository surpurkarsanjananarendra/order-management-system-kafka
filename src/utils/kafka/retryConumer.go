package kafka

import (
	"context"
	"fmt"
	"time"

	"order_management_system/src/utils/configs"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// retryConsumerHandler is a sarama.ConsumerGroupHandler for a single retry topic.
// Before invoking the upstream handler it sleeps for the configured delay, which
// gives transient errors (network blips, downstream downtime) time to recover.
type retryConsumerHandler struct {
	inner  sarama.ConsumerGroupHandler
	delay  time.Duration
	logger *logrus.Logger
}

func (r *retryConsumerHandler) Setup(s sarama.ConsumerGroupSession) error {
	return r.inner.Setup(s)
}
func (r *retryConsumerHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return r.inner.Cleanup(s)
}

func (r *retryConsumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		r.logger.WithFields(logrus.Fields{
			"retry_topic": msg.Topic,
			"partition":   msg.Partition,
			"offset":      msg.Offset,
			"delay":       r.delay,
		}).Info("retry consumer sleeping before reprocessing")

		// Honor the retry delay. This is deliberately blocking per-message:
		// a retry topic is low-throughput by design.
		select {
		case <-time.After(r.delay):
		case <-session.Context().Done():
			return nil
		}

		// Delegate to the inner handler (OrderConsumerGroup) which handles
		// the actual processing and will re-route to the next retry/DLQ on failure.
		if err := r.inner.(*OrderConsumerGroup).processMessage(session, msg); err != nil {
			r.logger.WithError(err).Warn("retry attempt failed")
		}
	}
	return nil
}

// StartRetryConsumer spins up one consumer group per retry topic.
// Each hop uses a distinct consumer group ID so that offsets are tracked
// independently and messages aren't re-consumed on the wrong hop.
func StartRetryConsumer(
	ctx context.Context,
	baseTopic string,
	retryConfig RetryConfig,
	callback KeyValueCallback,
	dlqRouter *DLQRouter,
	logger *logrus.Logger,
) {
	retryTopics := retryConfig.TopicNames(baseTopic)

	for i, topic := range retryTopics {
		delay := retryConfig.Delays[i]
		hopTopic := topic // capture for goroutine
		hopDelay := delay // capture
		hopIndex := i + 1 // human-readable

		go func() {
			logger.WithFields(logrus.Fields{
				"retry_topic": hopTopic,
				"delay":       hopDelay,
				"hop":         hopIndex,
			}).Info("starting retry consumer")

			startRetryHop(ctx, hopTopic, hopDelay, callback, dlqRouter, logger)
		}()
	}
}

func startRetryHop(
	ctx context.Context,
	topic string,
	delay time.Duration,
	callback KeyValueCallback,
	dlqRouter *DLQRouter,
	logger *logrus.Logger,
) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	// Retry topics process slowly by design — give more time per message.
	config.Consumer.MaxProcessingTime = 5 * time.Minute
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 2 * time.Second
	config.Version = sarama.V3_6_0_0

	cfg, err := configs.Get("application")
	if err != nil {
		panic(err)
	}

	baseGroupID := cfg.GetString("kafka.consumer_group")
	if baseGroupID == "" {
		baseGroupID = "order-consumer-group-v1"
	}
	// Unique group per retry topic: order-consumer-group-v1-retry-orders.events.retry-1
	groupID := fmt.Sprintf("%s-retry-%s", baseGroupID, topic)

	group, err := sarama.NewConsumerGroup(GetBrokers(), groupID, config)
	if err != nil {
		panic(fmt.Sprintf("failed to create retry consumer group for %s: %v", topic, err))
	}
	defer group.Close()

	innerHandler := &OrderConsumerGroup{
		dlqRouter: dlqRouter,
		logger:    logger,
	}
	if err := innerHandler.ConfigureCallback(KeyValueCallback(callback)); err != nil {
		panic(err)
	}

	handler := &retryConsumerHandler{
		inner:  innerHandler,
		delay:  delay,
		logger: logger,
	}

	for {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.WithFields(logrus.Fields{
				"retry_topic": topic,
				"error":       err.Error(),
			}).Error("retry consumer error, retrying in 5s")
			time.Sleep(5 * time.Second)
		}
		if ctx.Err() != nil {
			logger.WithField("retry_topic", topic).Info("context cancelled, stopping retry consumer")
			return
		}
	}
}
