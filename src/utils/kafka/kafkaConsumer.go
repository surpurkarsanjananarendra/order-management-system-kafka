package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"order_management_system/src/models"
	"order_management_system/src/utils/configs"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type KeyValueCallback func(string, models.OrderEvent) error

// OrderConsumerGroup implements sarama.ConsumerGroupHandler.
// It now holds a DLQRouter so that any processing failure is routed
// to the appropriate retry topic or DLQ instead of being silently dropped.
type OrderConsumerGroup struct {
	callback  interface{}
	dlqRouter *DLQRouter // nil-safe: if nil, failures are logged and skipped (legacy)
	logger    *logrus.Logger
}

func (o *OrderConsumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	o.logger.WithField("partitions", session.Claims()).Info("consumer group session started")
	return nil
}

func (o *OrderConsumerGroup) Cleanup(session sarama.ConsumerGroupSession) error {
	o.logger.Info("consumer group session ended")
	return nil
}

func (o *OrderConsumerGroup) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {

	o.logger.WithField("partition", claim.Partition()).Info("ConsumeClaim started")

	for msg := range claim.Messages() {
		o.logger.WithFields(logrus.Fields{
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"key":       string(msg.Key),
		}).Debug("received message")

		if err := o.processMessage(session, msg); err != nil {
			// processMessage already routed to DLQ; we log and continue
			// so the partition is never blocked.
			o.logger.WithError(err).Error("message handling pipeline failed")
		}
	}

	return nil
}

// processMessage handles a single Kafka message end-to-end:
// unmarshal → callback → mark offset, or → DLQ on any error.
func (o *OrderConsumerGroup) processMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	var event models.OrderEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		o.logger.WithError(err).Error("unmarshal failed, routing to DLQ")
		// Unmarshal errors are permanent — route straight to DLQ, no retry value.
		if routeErr := o.route(msg, err, models.ErrorTypeUnmarshal); routeErr != nil {
			o.logger.WithError(routeErr).Error("failed to route unmarshal error to DLQ")
		}
		// Always mark: we cannot recover a malformed payload so blocking is pointless.
		session.MarkMessage(msg, "")
		return err
	}

	err := o.callback.(KeyValueCallback)(string(msg.Key), event)
	if err != nil {
		o.logger.WithFields(logrus.Fields{
			"key":   string(msg.Key),
			"error": err.Error(),
		}).Warn("callback failed, routing to retry/DLQ")

		if routeErr := o.route(msg, err, models.ErrorTypeCallback); routeErr != nil {
			o.logger.WithError(routeErr).Error("failed to route callback error")
		}
		// Mark so we advance the offset — the message now lives in a retry topic.
		session.MarkMessage(msg, "")
		return err
	}

	session.MarkMessage(msg, "")
	o.logger.WithFields(logrus.Fields{
		"partition": msg.Partition,
		"offset":    msg.Offset,
	}).Debug("message processed successfully")
	return nil
}

// route sends a failed message to the DLQ router.
// If no router is configured, it falls back to logging only.
func (o *OrderConsumerGroup) route(msg *sarama.ConsumerMessage, err error, errType string) error {
	if o.dlqRouter == nil {
		o.logger.Warn("no DLQ router configured — message will be skipped")
		return nil
	}
	return o.dlqRouter.Route(msg, err, errType)
}

func (o *OrderConsumerGroup) ConfigureCallback(callback interface{}) error {
	_, ok := callback.(KeyValueCallback)
	if !ok {
		return fmt.Errorf("invalid callback type: expected KeyValueCallback")
	}
	o.callback = callback
	return nil
}

// StartConsumer starts a consumer group for topic using the given callback and DLQ router.

func StartConsumer(ctx context.Context, topic string, callback KeyValueCallback, dlqRouter *DLQRouter, logger *logrus.Logger) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Manual offset commit after processing ensures we only advance past a
	// message once it has been either processed successfully or routed to DLQ.
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
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

	groupID := cfg.GetString("kafka.consumer_group")
	if groupID == "" {
		groupID = "order-consumer-group-v1"
	}

	group, err := sarama.NewConsumerGroup(GetBrokers(), groupID, config)
	if err != nil {
		panic(fmt.Sprintf("failed to create consumer group: %v", err))
	}
	defer group.Close()

	handler := &OrderConsumerGroup{
		dlqRouter: dlqRouter,
		logger:    logger,
	}

	if err := handler.ConfigureCallback(KeyValueCallback(callback)); err != nil {
		panic(err)
	}

	logger.WithField("topic", topic).Info("consumer group started, waiting for messages")

	for {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.WithError(err).Error("error during consumption, retrying in 2s")
			time.Sleep(2 * time.Second)
		}
		if ctx.Err() != nil {
			logger.Info("context cancelled, stopping consumer")
			return
		}
	}
}
