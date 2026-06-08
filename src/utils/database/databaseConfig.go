package database

import (
	"errors"
	"fmt"
	"order_management_system/src/constants"
	"order_management_system/src/models"
	"sync"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *models.Database
var once sync.Once

// InitDB opens a PostgreSQL connection via GORM using DATABASE_URL
// InitDB initializes the database only once
func InitDB() error {

	var initErr error

	once.Do(func() {
		dsn := fmt.Sprintf(constants.DSNString, "localhost", "5432", "order_management_system", "postgres", "sanjupost", "Asia/Kolkata")
		if dsn == "" {
			initErr = errors.New("DATABASE_URL is not Set")
			return
		}

		gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			initErr = fmt.Errorf("open database: %w", err)
			return
		}

		db = &models.Database{
			DB: gdb,
		}
	})

	return initErr
}

// GetDB returns the shared model.Database. Call InitDB first.
func GetDB() *models.Database {
	return db
}
