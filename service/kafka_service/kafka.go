package kafka_service

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// SubmittBuffer which is open to the submitter
var SubmittBuffer string

// Consumer is a service that read from the kafka service and re-organize the data and then, submit into the rocksdb
func Consumer() {
	fmt.Printf("consumer_test")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Printf("consumer_test create consumer error %s\n", err.Error())
		return
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("blocknumber", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("try create partition_consumer error %s\n", err.Error())
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// 在每个周期，抓取kafka的信息
			// 如果是这个周期的第一条信息，存下来。
			// 如果在这个周期内继续接到其他的信息和存下来的信息相同，则丢弃这条信息
			// 如过在这个周期内继续接到其他的信息和存下来的信息不同，则可能数据出现了问题，则应该记录下错误信息
			// 周期结束发送这条信息
			var result map[string]interface{}
			json.Unmarshal([]byte(msg.Value), &result)
			params := result["params"].(map[string]interface{})
			var paramsInt int64 = int64(params["timestamp"].(float64))
			// fmt.Println(paramsInt)
			fmt.Println(paramsInt - time.Now().Unix())
			if paramsInt-time.Now().Unix() > -10 {
				// 有效信息
				if SubmittBuffer == "" {
					fmt.Println("the submitbuffer is null")
					SubmittBuffer = fmt.Sprint(params["value"].(float64))
				} else {
					fmt.Println("comparing")
					fmt.Println(SubmittBuffer)
					fmt.Println(fmt.Sprint(params["value"].(float64)))
					res := fmt.Sprint(params["value"].(float64)) == SubmittBuffer
					fmt.Println(res)
					if res == false {
						log.Fatalln("DATA IN NODES ARE NOT SAME")
					}
				}
			}
			fmt.Printf("msg offset: %d, partition: %d, timestamp: %s, value: %s\n", msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Printf("err :%s\n", err.Error())
		}
	}
}
