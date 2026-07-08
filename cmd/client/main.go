package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Zallu35/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Zallu35/learn-pub-sub-starter/internal/pubsub"
	"github.com/Zallu35/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connString := "amqp://guest:guest@localhost:5672/"
	rmqConnection, err := amqp.Dial(connString)
	if err != nil {
		fmt.Printf("Error connecting to RabbitMQ server: %v\n", err)
		return
	}
	defer rmqConnection.Close()
	fmt.Printf("Established connection to RabbitMQ server successfully\n")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Invalid username: %v", err)
		return
	}

	theChannel, transQueue, err := pubsub.DeclareAndBind(rmqConnection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient)
	if theChannel != nil {
		fmt.Print("Channel Good")
	}
	if transQueue.Name != "" {
		fmt.Print("Queue Good")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	fmt.Printf("Closing client")
}
