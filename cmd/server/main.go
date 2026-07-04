package main

import (
	"fmt"
	"os"
	"os/signal"

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
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	fmt.Printf("Closing server")
}
