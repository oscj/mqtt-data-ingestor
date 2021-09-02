package main

import (
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mongodb.org/mongo-driver/bson"
)

// subscribes client to target topic
func sub(client mqtt.Client, targetTopic string, msgHandler mqtt.MessageHandler) {
	token := client.Subscribe(targetTopic, 1, msgHandler)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", targetTopic)
}

// client handlers
func MakeConnectedHandler(config Config, dbClient MongoClient) func(mqtt.Client) {
	return func(client mqtt.Client) {
		fmt.Println("Connected")
		sub(client, config.TargetTopic, MakeMessageHandler(config, dbClient))
	}
}

func MakeConnectionLostHandler(dbClient MongoClient) func(mqtt.Client, error) {
	return func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
		dbClient.disconnect()
	}
}

func MakeMessageHandler(config Config, dbClient MongoClient) func(mqtt.Client, mqtt.Message) {
	return func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\nInserting Message...\n", msg.Payload(), msg.Topic())

		var bSensorDoc interface{}
		err := bson.UnmarshalExtJSON(msg.Payload(), true, &bSensorDoc)
		if err != nil {
			log.Fatal(err)
		}

		err = dbClient.insertDocToCollection(
			config.TargetDB,
			config.TargetCollection,
			bSensorDoc,
		)

		if err != nil {
			fmt.Println("Error inserting incoming msg to db")
		} else {
			fmt.Println("Successfully inserted incoming msg to db")
		}
	}
}
