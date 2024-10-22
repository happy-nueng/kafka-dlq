## Kafka
เข้าไปที่ Container kafka ของเราด้วยคำสั่งนี้
```bash
docker exec -it kafka sh -c "cd /opt/kafka/bin"
```
คำสั่ง List Topic
```bash
./kafka-topics.sh --bootstrap-server=localhost:9092 --list
```
การ Produce message
```bash
./kafka-console-producer.sh  --broker-list localhost:9092  --topic my-topic
```
จากนั้นทำการพิมพ์ Message ที่ต้องการ Procude ลงไปได้เลย