package validations

import (
	"fmt"
	"order_management_system/src/constants"
	"order_management_system/src/models"
	"strings"

	"github.com/go-playground/validator/v10"
)

var bffValidator *validator.Validate

func FormatValidationErrors(err error) ([]models.ErrorMessage, string) {
	var validationErrors []models.ErrorMessage
	var validationErrorsStr string

	for _, err := range err.(validator.ValidationErrors) {
		var errorMsg string
		fieldName := err.Field()
		if err.Tag() == "required" {
			fieldName = strings.ToLower(fieldName)
			errorMsg = fmt.Sprintf(constants.RequiredFieldError, fieldName)
		} else {
			switch err.Field() {

			default:
				errorMsg = fmt.Sprintf(constants.InvalidValueError, err.Field())
			}
		}

		validationErrors = append(validationErrors, models.ErrorMessage{
			Key:          fieldName,
			ErrorMessage: errorMsg,
		})
		validationErrorsStr += fieldName + " is invalid; "
	}

	return validationErrors, validationErrorsStr
}

func GetBFFValidator() *validator.Validate {
	return bffValidator
}
