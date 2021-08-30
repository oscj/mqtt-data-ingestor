package main

import (
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v2"
)

var (
	messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	}

	connectedHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
		fmt.Println("Connected")
		sub(client, "sensor/dht22-1", messagePubHandler)

	}

	connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}
)

type Config struct {
	BrokerAddress  string `yaml:"broker_address"`
	BrokerPort     int    `yaml:"broker_port"`
	ClientId       string `yaml:"client_id"`
	ClientPassword string `yaml:"client_password"`
	ClientUserName string `yaml:"client_username"`
	TargetTopic    string `yaml:"target_topic"`
}

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

	for {
	}
}

func sub(client mqtt.Client, targetTopic string, msgHandler mqtt.MessageHandler) {
	token := client.Subscribe(targetTopic, 1, msgHandler)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", targetTopic)
}
