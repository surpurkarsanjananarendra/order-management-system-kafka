package main

import (
	"log"

	"order_management_system/src/utils/database"

	"order_management_system/src/models"
)

func main() {
	err := database.InitDB()
	if err != nil {
		log.Fatalf("Failed to Initialize Database: %v", err)
	}

	db := database.GetDB().DB

	if err := db.AutoMigrate(&models.Orders{}); err != nil {
		log.Fatalf("error in automigrating schema: %s", err.Error())
	}

	if err := db.AutoMigrate(); err != nil {
		log.Fatalf("error in automigrating schema: %s", err.Error())
	}

	log.Print("Database Migrated successfully")

}
