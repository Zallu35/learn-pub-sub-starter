package main

import (
	"fmt"

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

	publishingChannel, err := rmqConnection.Channel()
	if err != nil {
		fmt.Printf("Error making new channel: %v\n", err)
		return
	}

	fmt.Printf("Established connection to RabbitMQ server successfully\n")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Invalid username: %v", err)
		return
	}

	gs := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(rmqConnection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient, handlerPause(gs))
	if err != nil {
		fmt.Printf("SubscribeJSON error: %v", err)
		return
	}

	err = pubsub.SubscribeJSON(rmqConnection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gs))
	if err != nil {
		fmt.Printf("SubscribeJSON error (ArmyMove): %v", err)
		return
	}

Mainloop:
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0] {
		case "spawn":
			err = gs.CommandSpawn(userInput)
			if err != nil {
				fmt.Printf("Error spawning: %v", err)
			}
		case "move":
			moveVal, err := gs.CommandMove(userInput)
			if err != nil {
				fmt.Printf("Error moving: %v", err)
			}
			err = pubsub.PublishJSON(publishingChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, moveVal)
			if err == nil {
				fmt.Println("Move published!")
			}
		case "status":
			gs.CommandStatus()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			break Mainloop
		default:
			fmt.Println("Unknown command, type 'help' for a list of commands")

		}
	}
	/*
	   sigChan := make(chan os.Signal, 1)
	   signal.Notify(sigChan, os.Interrupt)
	   <-sigChan
	   fmt.Printf("Closing client")
	*/
}
