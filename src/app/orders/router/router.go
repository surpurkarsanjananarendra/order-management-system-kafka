package router

import (
	"order_management_system/src/utils/kafka"
	"orders/commons/constants"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	files "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"orders/business"
	"orders/docs"
	"orders/handlers"
	"orders/repository"

	"gorm.io/gorm"
)

func GetRouter(db *gorm.DB, topic string, producer *kafka.Producer, logger *logrus.Logger) *gin.Engine {
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	docs.SwaggerInfo.Title = "garage management system"
	router.GET("/swagger/*any", ginSwagger.WrapHandler(files.Handler))

	router.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowMethods:    []string{"POST", "GET", "PUT", "DELETE"},
		AllowHeaders:    []string{"Authorization", "Content-type", "Origin"},
	}))

	kafkaRepo := repository.NewKafkaRepository(topic,producer)
	kafkaService := business.NewPublishOrderService(kafkaRepo)
	kafkaHandler := handlers.NewPublishOrderHandler(kafkaService)

	kafkaGroup := router.Group(constants.ServiceRoutePrefix)
	{
		kafkaGroup.POST(constants.PublishOrder, kafkaHandler.PublishOrder)
	}

	return router
}
