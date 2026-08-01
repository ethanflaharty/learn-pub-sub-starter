package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "applications/json", Body: bytes})
	if err != nil {
		return err
	}

	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	chn, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := chn.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	var v T
	go func() {
		for delivery := range deliveryChan {
			err = json.Unmarshal(delivery.Body, &v)
			if err != nil {
				log.Fatalf("")
			}
			handler(v)
			delivery.Ack(false)
		}
	}()

	return nil
}
