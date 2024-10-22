package main

import (
	"context"
	"fmt"
	kafkadlq "kafka_dlq"
	"log"
	"time"
)

func main() {
	// Kafka config
	kafkaBrokers := []string{"localhost:9092"}
	kafkaTopic := "your-topic"
	kafkaGroupID := "your-group-id"

	// MongoDB config
	mongoConnectionString := "mongodb://root:root@localhost:27017"
	mongoDatabase := "kafka-dlq"
	mongoCollection := "messages"

	// ตั้งค่า Kafka Reader และ Writer
	reader := kafkadlq.NewKafkaReader(kafkaBrokers, []string{kafkaTopic, kafkaTopic + "-retry"}, kafkaGroupID)
	writer := kafkadlq.NewKafkaWriter(kafkaBrokers, kafkaTopic+"-retry")

	// ตั้งค่า MongoDB Client และ Collection
	mongoClient, err := kafkadlq.NewMongoClient(mongoConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.TODO())

	collection := kafkadlq.GetMongoCollection(mongoClient, mongoDatabase, mongoCollection)

	// สร้าง DLQ Handler
	dlqHandler := kafkadlq.NewDLQHandler(reader, writer, collection, 3, 2*time.Second)

	// เริ่มการประมวลผลข้อความ โดยส่งฟังก์ชันประมวลผล message เข้าไป
	dlqHandler.ProcessMessages(func(msg []byte) error {
		// ฟังก์ชันสำหรับประมวลผลข้อความ (จำลอง error)
		return fmt.Errorf("simulated processing error")
	})
}
