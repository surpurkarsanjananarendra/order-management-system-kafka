package kafka

import "encoding/json"

func EncodeMessage(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}
