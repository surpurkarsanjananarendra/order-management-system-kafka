package main

import (
	"fmt"
	"log"
	"order_management_system/src/utils/database"
	"order_management_system/src/utils/kafka"
	"orders/router"
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// @title Producer Service API
// @version 1.0
// @description Producer APIs for Publishing Events in Order Management System
// @query.collection.format multi
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Producer
// @x-extension-openapi {"example": "value on a json format"}
func main() {
	err := godotenv.Load("C:/Users/Coditas-Admin/Documents/Coditas Internship/Order_Management_System/.env")
	if err != nil {
		log.Println(".env file not found, using system env")
	} else {
		fmt.Println("ENV loaded successfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	}

	orderTopic := os.Getenv("KAFKA_ORDER_TOPIC")

	err = database.InitDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		PrettyPrint: true,
	})

	db := database.GetDB()
	producer, err := kafka.NewProducer() //get the new producer so as to publish order events into kafka

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	startRouter(db.DB, orderTopic, producer, logger)
}

func startRouter(db *gorm.DB, topic string, producer *kafka.Producer, logger *logrus.Logger) {
	router := router.GetRouter(db, topic, producer, logger)
	router.Run(":9090")
}
