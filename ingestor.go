package main

import (
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mongodb.org/mongo-driver/bson"
	"gopkg.in/yaml.v2"
)

type Config struct {
	BrokerAddress    string `yaml:"broker_address"`
	BrokerPort       int    `yaml:"broker_port"`
	ClientId         string `yaml:"client_id"`
	ClientPassword   string `yaml:"client_password"`
	ClientUserName   string `yaml:"client_username"`
	TargetTopic      string `yaml:"target_topic"`
	DBUri            string `yaml:"db_uri"`
	TargetDB         string `yaml:"target_db"`
	TargetCollection string `yaml:"target_collection"`
}

var config Config

func main() {

	cfgData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// default config
	config = Config{
		BrokerPort:       1883,
		TargetDB:         "test",
		TargetCollection: "test",
	}

	err = yaml.Unmarshal(cfgData, &config)
	if err != nil {
		log.Fatal(err)
	}

	// mqtt client
	broker := config.BrokerAddress
	port := config.BrokerPort
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtt://%s:%d", broker, port))
	opts.SetClientID(config.ClientId)
	opts.SetUsername(config.ClientUserName)
	opts.SetPassword(config.ClientPassword)
	opts.OnConnect = connectedHandler
	opts.OnConnectionLost = connectionLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// run forever
	for {
	}
}

// subscribes client to target topic
func sub(client mqtt.Client, targetTopic string, msgHandler mqtt.MessageHandler) {
	token := client.Subscribe(targetTopic, 1, msgHandler)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", targetTopic)
}

// client handlers
var (
	connectedHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
		fmt.Println("Connected")
		sub(client, config.TargetTopic, messagePubHandler)
	}

	connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}

	messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\nInserting Message...\n", msg.Payload(), msg.Topic())

		var bSensorDoc interface{}
		err := bson.UnmarshalExtJSON(msg.Payload(), true, &bSensorDoc)
		if err != nil {
			log.Fatal(err)
		}

		dbClient, err := get_client(config.DBUri)

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

		dbClient.disconnect()
	}
)
