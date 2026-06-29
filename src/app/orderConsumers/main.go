package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"order-consumers/repository"
	"order_management_system/src/utils/database"
	"order_management_system/src/utils/kafka"

	"order_management_system/src/utils/configs"

	"github.com/sirupsen/logrus"
)

func main() {
	configs.Init([]string{
		"C:/Users/Coditas-Admin/Documents/Coditas Internship/Order_Management_System/src/config",
	})

	cfg, err := configs.Get("application")
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{PrettyPrint: true})

	if err := database.InitDB(); err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	fmt.Println("=== DB initialized ===")

	db := database.GetDB()
	if db == nil || db.DB == nil {
		log.Fatal("database instance is nil — check InitDB")
	}

	topic := cfg.GetString("kafka.order_topic")
	if topic == "" {
		log.Fatal("kafka.order_topic not set in config")
	}

	producer, err := kafka.NewProducer()
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	// ── Retry config ─────────────────────────────────────────────────────────
	// Change delays here to tune retry behaviour for your SLA.
	retryConfig := kafka.DefaultRetryConfig() // 5s · 30s · 120s → DLQ

	// ── DLQ router (wires into the main consumer) ────────────────────────────
	consumerGroup := cfg.GetString("kafka.consumer_group")
	if consumerGroup == "" {
		consumerGroup = "order-consumer-group-v1"
	}

	dlqRouter := kafka.NewDLQRouter(producer, topic, consumerGroup, retryConfig, logger)

	callback := repository.ConsumeOrder(ctx, db.DB)

	// ── Main consumer ────────────────────────────────────────────────────────
	go kafka.StartConsumer(ctx, topic, callback, dlqRouter, logger)
	logger.WithField("topic", topic).Info("main consumer started")

	// ── Retry consumers (one per retry topic) ───────────────────────────────
	// Each hop has its own consumer group, its own delay, and its own DLQ router.
	// On exhaustion the DLQ router routes to the next hop or final DLQ automatically.
	go kafka.StartRetryConsumer(ctx, topic, retryConfig, callback, dlqRouter, logger)
	logger.Info("retry consumers started")

	// ── DLQ processor ────────────────────────────────────────────────────────
	dlqProcessorConfig := kafka.DefaultDLQProcessorConfig()
	// Uncomment to enable automatic replay once the root cause is fixed:
	// dlqProcessorConfig.EnableReplay = true
	dlqProcessor := kafka.NewDLQProcessor(producer, dlqProcessorConfig, logger)

	go kafka.StartDLQProcessor(ctx, topic, dlqProcessor, logger)
	logger.Info("DLQ processor started")

	// ── Graceful shutdown ────────────────────────────────────────────────────
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	logger.Info("shutdown signal received, stopping all consumers...")
	cancel()
}
