package kafkadlq

import (
	"github.com/segmentio/kafka-go"
)

func NewKafkaReader(brokers []string, topics []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupTopics: topics,
		GroupID:     groupID,
	})
}
