package main

import (
	"fmt"

	"github.com/Zallu35/learn-pub-sub-starter/internal/gamelogic"
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

	secondChannel, transQueue, err := pubsub.DeclareAndBind(rmqConnection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable)
	if secondChannel != nil {
		fmt.Println("Channel Good")
	}
	if transQueue.Name != "" {
		fmt.Println("Queue Good")
	}

	gamelogic.PrintServerHelp()

MainLoop:
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0] {
		case "pause":
			fmt.Println("Sending pause")
			err = pubsub.PublishJSON(myNewChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume")
			err = pubsub.PublishJSON(myNewChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "help":
			gamelogic.PrintServerHelp()
		case "quit":
			fmt.Println("Exiting")
			break MainLoop
		default:
			fmt.Println("Unknown command, type 'help' for a list of commands")
		}
	}
	/*
	   sigChan := make(chan os.Signal, 1)
	   signal.Notify(sigChan, os.Interrupt)
	   <-sigChan
	   fmt.Printf("Closing server")
	*/
}
