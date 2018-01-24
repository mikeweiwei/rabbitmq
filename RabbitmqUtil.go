package rabbitmq

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
)


const (

	mqurl =""

)


var conn *amqp.Connection
var channel *amqp.Channel

//异常处理
func failOnErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s:%s", msg, err)
		panic(fmt.Sprintf("%s:%s", msg, err))
	}
}

//连接mq
func mqConnect() {
	var err error
	conn, err = amqp.Dial(mqurl)
	failOnErr(err, "failed to connect tp rabbitmq")

	channel, err = conn.Channel()
	failOnErr(err, "failed to open a channel")
}

//发送 返回发送状态
func Push(exchange string,queueName string,message string) bool{

	if channel == nil {
		mqConnect()
	}
	e := channel.Publish(exchange, queueName, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	})
	if e != nil {
		return false
	}else{
		return true
	}
}


//监听 返回<-chan amqp.Delivery
func Receive(queueName string) (<-chan amqp.Delivery, error) {
	if channel == nil {
		mqConnect()
	}
	msgs, err := channel.Consume(queueName, "", true, false, false, true, nil)

	failOnErr(err, "")

	return msgs,err

}
//poll方式获取消息
func POll(queueName string) (string, error) {

	if channel == nil {
		mqConnect()
	}

	delivery, _, err := channel.Get(queueName, true)

	mes := string(delivery.Body[:])

	failOnErr(err, "")

	return mes,err

}
