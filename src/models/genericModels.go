package models

type ErrorMessage struct {
	Key          string `json:"key,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

type ErrorAPIResponse struct {
	Message ErrorMessage `json:"errors,omitempty"`
	Error   string       `json:"error,omitempty"`
}