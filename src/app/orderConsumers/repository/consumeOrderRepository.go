package repository

import (
	"context"
	"order_management_system/src/utils/kafka"

	// "encoding/json"
	"fmt"
	"order-consumers/commons/constants"

	"order_management_system/src/models"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Returns a callback closure that captures db
// func ConsumeOrder(ctx context.Context, db *gorm.DB) func(map[string]any) { // this is the wrapper function for the callback func so that db is upated for every callback
// 	return func(payload map[string]any) { //closure of callback fun just bcz kafka knows only callback not the db and hence to make sure the db is updated for each callback
// 		start := time.Now()
// 		logger := logrus.New()

//         //we already have data in struct form in payload with address, just pass it as it is wihout &, it will work!!
// 		result := db.WithContext(ctx).Table(constants.OrdersTableName).Create(payload)
// 		if result.Error != nil {
// 			fmt.Println("DB insert error:", result.Error)
// 			return
// 		}

// 		logger.WithFields(logrus.Fields{
// 			"latency": time.Since(start).Milliseconds(),
// 		}).Info(constants.EventsConsumedSuccess)

// 		fmt.Println("Event consumed and saved to DB:", payload)
// 	}
// }

func ConsumeOrder(
	ctx context.Context,
	db *gorm.DB,
) kafka.KeyValueCallback {

	return func(
		key string,
		event models.OrderEvent,
	) error {

		start := time.Now()

		result := db.WithContext(ctx).
			Table(constants.OrdersTableName).
			Create(event)

		if result.Error != nil {
			return result.Error
		}

		logger := logrus.New()

		logger.WithFields(logrus.Fields{
			"latency": time.Since(start).Milliseconds(),
		}).Info(constants.EventsConsumedSuccess)

		fmt.Println("saved:", key)

		return nil
	}
}
