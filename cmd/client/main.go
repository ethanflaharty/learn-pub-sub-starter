package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error making connection: %v", err)
		return
	}

	defer connection.Close()

	fmt.Println("Connection was successful")

	cntnChan, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating connection channel: %v", err)
		return
	}

	err = pubsub.PublishJSON(cntnChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("Error publishing json: %v", err)
		return
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error getting client username: %v", err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("Error declaring and binding: %v", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Shutting program down...")

	defer connection.Close()
}
