package main

import (
	"log"
	"order_management_system/src/utils/database"
	"order_management_system/src/utils/kafka"
	"orders/router"

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
	err := database.InitDB()
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
	startRouter(db.DB, producer, logger)
}

func startRouter(db *gorm.DB, producer *kafka.Producer, logger *logrus.Logger) {
	router := router.GetRouter(db, producer, logger)
	router.Run(":9090")
}
