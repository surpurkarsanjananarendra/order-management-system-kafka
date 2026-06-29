package models

import "time"

// DLQMetadata is stored as Kafka headers on every message routed to a retry or DLQ topic.
// It lets operators understand exactly why and when a message failed.
type DLQMetadata struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalPartition int32     `json:"original_partition"`
	RetryCount        int       `json:"retry_count"`
	MaxRetries        int       `json:"max_retries"`
	ErrorMessage      string    `json:"error_message"`
	ErrorType         string    `json:"error_type"` // e.g. "unmarshal_error", "callback_error"
	FailedAt          time.Time `json:"failed_at"`
	ConsumerGroup     string    `json:"consumer_group"`
}

// Kafka header keys used to carry DLQ metadata alongside the original payload.
const (
	HeaderRetryCount        = "x-retry-count"
	HeaderOriginalTopic     = "x-original-topic"
	HeaderOriginalOffset    = "x-original-offset"
	HeaderOriginalPartition = "x-original-partition"
	HeaderErrorMessage      = "x-error-message"
	HeaderErrorType         = "x-error-type"
	HeaderFailedAt          = "x-failed-at"
	HeaderConsumerGroup     = "x-consumer-group"
	ErrorTypeUnmarshal      = "unmarshal_error"
	ErrorTypeCallback       = "callback_error"
	ErrorTypeUnknown        = "unknown_error"
)
