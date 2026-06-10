package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type OrderConsumerGroup struct {
	callback func(map[string]any)
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

func (o *OrderConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("=== ConsumeClaim started for partition:", claim.Partition(), "===")

	//this is infinite listening loop which will not exit unless sarama stops during rebalancing
	for msg := range claim.Messages() {
		fmt.Printf("Partition: %d | Offset: %d\n", msg.Partition, msg.Offset)
		fmt.Println("Raw Value:", string(msg.Value))

		//whatever messages or the data we got from kafka is in the JSON form
		//hence we unmarshall JSON to STRUCT and store it to the payload
		var payload map[string]any
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			fmt.Println("UNMARSHAL FAILED:", err)
			//tells kafka this msg has been processed and updates offset, after restart, it will process from he next offset not from first
			session.MarkMessage(msg, "")
			continue
		}

		fmt.Println("Payload received:", payload)
		o.callback(payload)

		session.MarkMessage(msg, "")
		fmt.Println("Message marked as processed")
	}
	return nil
}

func StartConsumer(topic string, callback func(map[string]any)) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//after every one second the commit is processed to offsets so that after restare it should avoid  reprocessing of the processed offsets
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	config.Consumer.Group.Session.Timeout = 20 * time.Second   // if it doesn't here anything upyo 20 seconds then it will assume that consumer is dead and we need to rebalancing
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second // after every 6 seconds consumer sends a heartbeat to the coordinator
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond // max time to process one message
	config.Net.DialTimeout = 10 * time.Second                  // timeout to connect to broker
	config.Net.ReadTimeout = 10 * time.Second                  // timeout to read from broker
	config.Net.WriteTimeout = 10 * time.Second                 // timeout to write to broker
	config.Metadata.Retry.Max = 5                              // retry fetching metadata 5 times
	config.Metadata.Retry.Backoff = 2 * time.Second            // wait 2s between retries

	config.Version = sarama.V2_6_0_0

	fmt.Println("=== Connecting to Kafka brokers:", Brokers, "===")

	group, err := sarama.NewConsumerGroup(Brokers, "order-consumer-group-v1", config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer group: %v", err))
	}
	defer group.Close()

	handler := &OrderConsumerGroup{callback: callback}
	ctx := context.Background()

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
