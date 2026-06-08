package handlers

import (
	"net/http"

	"orders/business"
	"orders/models"

	"github.com/gin-gonic/gin"
)

type PublishOrderHandler struct {
	service *business.PublishOrderService
}

func NewPublishOrderHandler(
	service *business.PublishOrderService,
) *PublishOrderHandler {

	return &PublishOrderHandler{
		service: service,
	}
}

// PublishOrder godoc
//
// @Summary Publish New Order
// @Description Publish order event to kafka
// @Tags Orders
// @Accept json
// @Produce json
// @Param request body models.BFFPublishOrderRequest true "order publish Request"
// @Success 201 {object} models.BFFPublishOrderRequest "order received successfully"
// @Failure 400 {object} models.ErrorAPIResponse "Invalid input payload"
// @Failure 409 {object} models.ErrorAPIResponse "Duplicate value in request"
// @Failure 500 {object} models.ErrorAPIResponse "Internal Server error"
// @Router /api/orders [post]
func (h *PublishOrderHandler) PublishOrder(
	ctx *gin.Context,
) {

	var request models.BFFPublishOrderRequest

	if err := ctx.ShouldBindJSON(&request); err != nil {

		ctx.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})

		return
	}

	err := h.service.PublishOrder(request)

	if err != nil {

		ctx.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})

		return
	}

	ctx.JSON(
		http.StatusOK,
		models.BFFPublishOrderResponse{
			Message: "Order Received Successfully",
		},
	)
}
