package main

import (
	"fmt"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

func main() {

	cfgData, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// default config
	config := Config{
		BrokerPort:       1883,
		TargetDB:         "test",
		TargetCollection: "test",
	}

	err = yaml.Unmarshal(cfgData, &config)
	if err != nil {
		log.Fatal(err)
	}

	dbClient, err := GetDbClient(config.DBUri)
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
	opts.OnConnectionLost = MakeConnectionLostHandler(*dbClient)
	opts.OnConnect = MakeConnectedHandler(config, *dbClient)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// run forever
	for {
	}
}
