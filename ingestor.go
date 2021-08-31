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
	BrokerAddress  string `yaml:"broker_address"`
	BrokerPort     int    `yaml:"broker_port"`
	ClientId       string `yaml:"client_id"`
	ClientPassword string `yaml:"client_password"`
	ClientUserName string `yaml:"client_username"`
	TargetTopic    string `yaml:"target_topic"`
	DBUri          string `yaml:"db_uri"`
}

var (
	messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	}
	connectedHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
		fmt.Println("Connected")

	}
	connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}
)

func main() {

	cfgData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	cfg := Config{
		BrokerPort: 1883,
	}
	err = yaml.Unmarshal(cfgData, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	broker := cfg.BrokerAddress
	port := cfg.BrokerPort
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("mqtt://%s:%d", broker, port))
	opts.SetClientID(cfg.ClientId)
	opts.SetUsername(cfg.ClientUserName)
	opts.SetPassword(cfg.ClientPassword)
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectedHandler
	opts.OnConnectionLost = connectionLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	//sub(client, "sensor/dht22-1", messagePubHandler)

	dbClient, err := get_client(cfg.DBUri)

	if err != nil {
		log.Fatal(err)
	}

	err = dbClient.insert_doc_to_collection("sensordatas", "dht22", bson.D{{Key: "temperature", Value: 10}, {Key: "humidity", Value: 20}})

	if err != nil {
		fmt.Println("error db")
		log.Fatal(err)
	}
	for {
	}
}

func sub(client mqtt.Client, targetTopic string, msgHandler mqtt.MessageHandler) {
	token := client.Subscribe(targetTopic, 1, msgHandler)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", targetTopic)
}
