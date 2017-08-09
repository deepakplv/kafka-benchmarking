// When running this script from a remote host set the server.properties with: advertised.listeners=PLAINTEXT://<PUBLIC_IP>:9092
package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os/signal"
	"os"
	"encoding/json"
	"time"
)

var brokers = []string{"54.219.130.71:9092"}
const topic = "Hello-Kafka"

func main() {
	mode := os.Getenv("mode")       // Use "export mode=c" or "export mode=p"
	if mode == "c" {
		fmt.Println("Starting consumer")
		ConsumeRecords()
	} else if mode == "p" {
		fmt.Println("Starting producer")
		ProduceRecords()
	} else {
		fmt.Println("Invalid Flag, Exiting")
		return
	}

        select{}
}

type Record struct {
	ID              uint64
	Value           map[string]interface{}
	InsertTime      int64
}

func ProduceRecords() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}

	// Handle process kill/interrupts and close the producer
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func() {
		<-c
		if err := producer.Close(); err != nil {
		    fmt.Println("Error closing async producer", err)
		}
		fmt.Println("Async Producer closed")
		os.Exit(1)
	}()

	go func() {
	        for err := range producer.Errors() {
		        fmt.Println("Failed to write message to topic:", err)
	        }
        }()

	inputChannel := producer.Input()
	var i uint64
	for i=0; i<100000; i++ {
		payload := map[string]interface{}{
		        "src": "972525626731",
		        "dst": "972502224696",
		        "prefix": "972502224696",
		        "url": "",
		        "method": "POST",
		        "text": "\u05dc\u05e7\u05d5\u05d7 \u05d9\u05e7\u05e8 \u05e2\u05e7\u05d1 \u05ea\u05e7\u05dc\u05d4 STOP",
		        "log_sms": "true",
		        "message_uuid": "ffe2bb44-d34f-4359-a7d7-217bf4e9f705",
		        "message_time": "2017-07-13 13:12:47.046303",
		        "carrier_rate": "0.0065",
		        "carrier_amount": "0.013",
		        "is_gsm": false,
		        "is_unicode": true,
		        "units": "2",
		        "auth_info": map[string]interface{}{
			        "auth_id": "MANZE1ODRHYWFIZGMXNJ",
				"auth_token": "NWRjNjU3ZDJhZDM0ZjE5NWE5ZWRmYTNmOGIzNGZm",
				"api_id": "de124d64-6186-11e7-920b-0600a1193e9b",
				"api_method": "POST",
				"api_name": "/api/v1/Message/",
				"account_id": "48844",
				"subaccount_id": "0",
				"parent_auth_id": "MANZE1ODRHYWFIZGMXNJ",
		        },
		}
		record := Record{
			ID: i,
			Value: payload,
			InsertTime: time.Now().UnixNano(),
		}
		recordBytes, _ := json.Marshal(record)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key: sarama.StringEncoder(record.ID),
			Timestamp: time.Now().UTC(),
			Value: sarama.ByteEncoder(recordBytes),
		}
		inputChannel <- msg
	}
	fmt.Println("Producer finished.")
}

func ConsumeRecords() {
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	// Handle process kill/interrupts and close the consumer
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			fmt.Println("Error closing the consumer", err)
		}

		fmt.Println("Consumer closed")
		os.Exit(0)
	}()

	fmt.Println("Waiting for records")

	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error getting partitionList ", err)
	}
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)
	        if err != nil {
	                fmt.Println("Error retrieving ConsumePartition ", err)
	        }
		// Keep listening to Messages channel for new records for each partition
		go func(pc sarama.PartitionConsumer) {
			var totalDiff, count uint64
			for message := range pc.Messages() {
				success, nanoSecDiff := processMessage(message)
				if success == false {
					fmt.Println("failure")
				}
				if nanoSecDiff != 0 {
					count += 1
					totalDiff += nanoSecDiff
				}
				if count % 1000 == 1 {
					fmt.Println("Running average latency for message no.: ", count, "is: ", totalDiff/count, " ns")
				}
			}
		}(pc)
	}
}

// This can be any external handler which does something with the record and returns status which
// can be used to commit offsetID.
func processMessage(message *sarama.ConsumerMessage) (bool, uint64) {
	//fmt.Println(
	//	"Key:", string(message.Key),
	//	"Value:", string(message.Value),
	//	"Offset:", message.Offset,
	//	"Topic:", message.Topic,
	//	"Partition:", message.Partition,
	//)
	record := Record{}
	json.Unmarshal(message.Value, &record)
	var nanoSecDiff uint64
	if record.InsertTime != 0 {
		nanoSecDiff = uint64(time.Now().UnixNano() - record.InsertTime)
	}
	return true, nanoSecDiff
}
