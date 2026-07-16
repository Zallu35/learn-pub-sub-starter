package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func (s SimpleQueueType) IsDurable() bool {
	return s == Durable
}

func (s SimpleQueueType) IsTransient() bool {
	return s == Transient
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshalledVal, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Error marshalling data: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: marshalledVal})
	if err != nil {
		return fmt.Errorf("Error publishing: %v", err)
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	theNewChannel, err := conn.Channel()
	if err != nil {
		return theNewChannel, amqp.Queue{}, err
	}
	theNewQueue, err := theNewChannel.QueueDeclare(queueName, queueType.IsDurable(), queueType.IsTransient(), queueType.IsTransient(), false, nil)
	if err != nil {
		return theNewChannel, theNewQueue, err
	}
	err = theNewChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return theNewChannel, theNewQueue, err
	}
	return theNewChannel, theNewQueue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) error {
	subChannel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	inboundMessages, err := subChannel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range inboundMessages {
			var data T
			json.Unmarshal(msg.Body, &data)
			handler(data)
			msg.Ack(false)
		}
	}()
	return nil
}
