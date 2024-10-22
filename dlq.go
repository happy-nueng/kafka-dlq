package kafkadlq

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type DLQHandler struct {
	Reader       *kafka.Reader
	Writer       *kafka.Writer
	Collection   *mongo.Collection
	MaxRetries   int
	RetryTimeout time.Duration
}

func NewDLQHandler(reader *kafka.Reader, writer *kafka.Writer, collection *mongo.Collection, maxRetries int, retryTimeout time.Duration) *DLQHandler {
	return &DLQHandler{
		Reader:       reader,
		Writer:       writer,
		Collection:   collection,
		MaxRetries:   maxRetries,
		RetryTimeout: retryTimeout,
	}
}

func (h *DLQHandler) ProcessMessages(processFunc func([]byte) error) {
	for {
		m, err := h.Reader.FetchMessage(context.TODO())
		if err != nil {
			log.Printf("Failed to fetch message: %v", err)
			continue
		}

		log.Printf("Received message: %s\n", string(m.Value))

		success := h.retryProcessing(m, processFunc)
		if !success {
			h.sendToDLQ(m)
		}

		if err := h.Reader.CommitMessages(context.TODO(), m); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}

func (h *DLQHandler) retryProcessing(m kafka.Message, processFunc func([]byte) error) bool {
	retryCount := getRetryCountFromHeaders(m.Headers)
	if retryCount < h.MaxRetries {
		if err := processFunc(m.Value); err == nil {
			return true
		}
		retryCount++
		err := h.Writer.WriteMessages(context.TODO(), kafka.Message{
			Key:   m.Key,
			Value: m.Value,
			Headers: []kafka.Header{
				{
					Key:   "retry-count",
					Value: []byte(fmt.Sprintf("%d", retryCount)),
				},
			},
		})
		log.Printf("Error processing message, retrying %d/%d", retryCount, h.MaxRetries)
		if err != nil {
			log.Printf("Failed to send message to retry topic: %v", err)
		} else {
			log.Printf("Message sent to retry topic: %s", string(m.Value))
		}
		return true
	}
	return false
}

func getRetryCountFromHeaders(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == "retry-count" {
			retryCount, err := strconv.Atoi(string(header.Value))
			if err != nil {
				return 0
			}
			return retryCount
		}
	}
	return 0
}

func (h *DLQHandler) sendToDLQ(m kafka.Message) {
	h.saveToMongoDB(m)
}

func (h *DLQHandler) saveToMongoDB(m kafka.Message) {
	dlqMessage := bson.M{
		"message": string(m.Value),
		"error":   "Failed to process and retry",
		"time":    time.Now(),
	}

	_, err := h.Collection.InsertOne(context.TODO(), dlqMessage)
	if err != nil {
		log.Printf("Failed to insert message to MongoDB: %v", err)
	} else {
		log.Printf("Message sent to DLQ in MongoDB: %s", string(m.Value))
	}
}
