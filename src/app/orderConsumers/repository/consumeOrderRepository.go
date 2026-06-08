package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"order-consumers/commons/constants"
	"order_management_system/src/models"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Returns a callback closure that captures db
func ConsumeOrder(ctx context.Context, db *gorm.DB) func(map[string]any) { // this is the wrapper function for the callback func so that db is upated for every callback
	return func(payload map[string]any) { //closure of callback fun just bcz kafka knows only callback not the db and hence to make sure the db is updated for each callback
		start := time.Now()
		logger := logrus.New()

		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			fmt.Println("Failed to marshal payload to JSON:", err)
			return
		}

		var order models.Orders
		if err := json.Unmarshal(jsonBytes, &order); err != nil {
			fmt.Println("Failed to unmarshal JSON into Orders struct:", err)
			return
		}

		result := db.WithContext(ctx).Table(constants.OrdersTableName).Create(&order)
		if result.Error != nil {
			fmt.Println("DB insert error:", result.Error)
			return
		}

		logger.WithFields(logrus.Fields{
			"latency": time.Since(start).Milliseconds(),
		}).Info(constants.EventsConsumedSuccess)

		fmt.Println("Event consumed and saved to DB:", payload)
	}
}
