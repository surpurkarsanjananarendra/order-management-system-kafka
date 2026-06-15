package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"order_management_system/src/config"
	"order_management_system/src/models"
	"time"

	"github.com/IBM/sarama"
)

type KeyValueCallback func(string, models.OrderEvent) error

type OrderConsumerGroup struct {
	callback interface{}
}

func (o *OrderConsumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("=== Setup: Consumer group session started ===")
	fmt.Println("Partitions assigned:", session.Claims())
	return nil
}

func (o *OrderConsumerGroup) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("=== Cleanup: Consumer group session ended ===")
	return nil
}

func (o *OrderConsumerGroup) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {

	fmt.Println("=== ConsumeClaim started for partition:", claim.Partition(), "===")

	for msg := range claim.Messages() {

		fmt.Printf(
			"Partition=%d Offset=%d Key=%s\n",
			msg.Partition,
			msg.Offset,
			string(msg.Key),
		)

		var event models.OrderEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			fmt.Println("UNMARSHAL FAILED:", err)
			session.MarkMessage(msg, "")
			continue
		}

		err := o.callback.(KeyValueCallback)(
			string(msg.Key),
			event,
		)

		if err != nil {
			fmt.Println("callback failed:", err)
			continue
		}

		session.MarkMessage(msg, "")
		fmt.Println("Message marked as processed")
	}

	return nil
}

func (o *OrderConsumerGroup) ConfigureCallback(callback interface{}) error {

	_, ok := callback.(KeyValueCallback)

	if !ok {
		return fmt.Errorf("invalid callback type")
	}

	o.callback = callback

	return nil
}

func StartConsumer(
	ctx context.Context,
	topic string,
	callback KeyValueCallback,
) {
	configs := sarama.NewConfig()
	configs.Consumer.Offsets.Initial = sarama.OffsetOldest
	//after every one second the commit is processed to offsets so that after restare it should avoid  reprocessing of the processed offsets
	configs.Consumer.Offsets.AutoCommit.Enable = true
	configs.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	configs.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	configs.Consumer.Group.Session.Timeout = 20 * time.Second   // if it doesn't here anything upyo 20 seconds then it will assume that consumer is dead and we need to rebalancing
	configs.Consumer.Group.Heartbeat.Interval = 6 * time.Second // after every 6 seconds consumer sends a heartbeat to the coordinator
	configs.Consumer.MaxProcessingTime = 500 * time.Millisecond // max time to process one message
	configs.Net.DialTimeout = 10 * time.Second                  // timeout to connect to broker
	configs.Net.ReadTimeout = 10 * time.Second                  // timeout to read from broker
	configs.Net.WriteTimeout = 10 * time.Second                 // timeout to write to broker
	configs.Metadata.Retry.Max = 5                              // retry fetching metadata 5 times
	configs.Metadata.Retry.Backoff = 2 * time.Second            // wait 2s between retries

	configs.Version = sarama.V3_6_0_0

	// fmt.Println("=== Connecting to Kafka brokers:", Brokers, "===")

	cfg, err := config.Get(".env")
	if err != nil {
		panic(err)
	}

	groupID := cfg.GetString("KAFKA_CONSUMER_GROUP")

	if groupID == "" {
		groupID = "order-consumer-group-v1"
	}

	group, err := sarama.NewConsumerGroup(
		GetBrokers(),
		groupID,
		configs,
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer group: %v", err))
	}
	defer group.Close()

	handler := &OrderConsumerGroup{}

	err = handler.ConfigureCallback(
		KeyValueCallback(callback),
	)

	if err != nil {
		panic(err)
	}

	fmt.Println("=== Consumer group started, waiting for messages... ===")

	for {
		err := group.Consume(ctx, []string{topic}, handler)
		if err != nil {
			fmt.Println("ERROR during consumption:", err)
			// wait before retrying so we don't spam the broker
			time.Sleep(2 * time.Second)
		}
		if ctx.Err() != nil {
			fmt.Println("Context cancelled, stopping consumer group")
			return
		}
	}
}
