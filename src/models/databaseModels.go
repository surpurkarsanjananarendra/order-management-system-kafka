package models

import (
	"time"

	"gorm.io/gorm"
)

type Orders struct {
	ID          uint64    `gorm:"column:id;primarykey;autoIncrement" json:"id"`
	OrderID     string    `gorm:"column:order_id" json:"order_id"`
	CustomerID  string    `gorm:"column:customer_id" json:"customer_id"`
	ProductName string    `gorm:"column:product_name" json:"product_name"`
	Quantity    uint64    `gorm:"column:quantity" json:"quantity"`
	Price       float64   `gorm:"column:price" json:"price"`
	CreatedAt   time.Time `gorm:"column:created_at" json:"created_at"`
}

type Database struct {
	DB *gorm.DB
}
