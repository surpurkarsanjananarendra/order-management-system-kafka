package models

type BFFPublishOrderRequest struct {
	OrderID     string  `json:"order_id" example:"101"`
	CustomerID  string  `json:"customer_id" example:"201"`
	ProductName string  `json:"product_name" example:"shampoo"`
	Quantity    uint64  `json:"quantity" example:"2"`
	Price       float64 `json:"price" example:"149.9"`
}

type BFFPublishOrderResponse struct {
	Message string `json:"message" example:"Order Received Successfully"`
}

