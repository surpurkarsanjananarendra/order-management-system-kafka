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

	"github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Init DB
	err := database.InitDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	fmt.Println("=== DB initialized ===")

	// 2. Setup logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{PrettyPrint: true})

	// 3. Get DB instance and validate
	db := database.GetDB()
	if db == nil {
		log.Fatal("GetDB() returned nil — InitDB may have failed silently")
	}
	if db.DB == nil {
		log.Fatal("db.DB is nil — models.Database not set correctly")
	}
	fmt.Println("=== DB instance verified ===")

	// 4. Start Kafka consumer in goroutine
	// ConsumeOrder(ctx, db.DB) builds the callback closure
	// StartConsumer calls it for every message
	go kafka.StartConsumer("temp", repository.ConsumeOrder(ctx, db.DB))
	fmt.Println("=== Kafka consumer started ===")

	// 5. Block until Ctrl+C or kill signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	logger.Info("Shutdown signal received, stopping consumer...")
	cancel()
}
