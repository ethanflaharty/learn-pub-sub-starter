package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	queue := amqp.Queue{}
	switch queueType {
	case Durable:
		queue, err = channel.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			return &amqp.Channel{}, amqp.Queue{}, err
		}
	case Transient:
		queue, err = channel.QueueDeclare(queueName, false, true, true, false, nil)
		if err != nil {
			return &amqp.Channel{}, amqp.Queue{}, err
		}
	default:
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("queueType must be durable or transient: %v", err)
	}

	channel.QueueBind(queueName, key, exchange, false, nil)

	return channel, queue, nil
}

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)
