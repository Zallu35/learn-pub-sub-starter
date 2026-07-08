package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Zallu35/learn-pub-sub-starter/internal/pubsub"
	"github.com/Zallu35/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connString := "amqp://guest:guest@localhost:5672/"
	rmqConnection, err := amqp.Dial(connString)
	if err != nil {
		fmt.Printf("Error connecting to RabbitMQ server: %v\n", err)
		return
	}
	defer rmqConnection.Close()
	fmt.Printf("Established connection to RabbitMQ server successfully\n")

	myNewChannel, err := rmqConnection.Channel()
	if err != nil {
		fmt.Printf("Error making new channel: %v\n", err)
		return
	}
	err = pubsub.PublishJSON(myNewChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		fmt.Printf("Error publishing JSON to channel: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	fmt.Printf("Closing server")
}
