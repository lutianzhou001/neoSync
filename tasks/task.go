package tasks

import (
	"fmt"

	"github.com/EDDYCJY/go-gin-example/service/kafka_service"
)

func typeof(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

// Submit is the function of gettint block number and submit to the sync server
func Submit() error {
	if kafka_service.SubmittBuffer != "" {
		// 2 提交
		fmt.Println("submitted")
	}
	// 3 清除要提交的信息
	kafka_service.SubmittBuffer = ""
	return nil
}
