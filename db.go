package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	*mongo.Client
}

// initializer
func get_client(uri string) (*MongoClient, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &MongoClient{client}, nil
}

func (client *MongoClient) disconnect_client() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client.Disconnect(ctx)
}

func (client *MongoClient) insert_doc_to_collection(database string, collection string, doc bson.D) error {
	sensorDatabase := client.Database(database)
	weatherSensorCollection := sensorDatabase.Collection(collection)

	_, err := weatherSensorCollection.InsertOne(context.Background(), doc)

	if err != nil {
		return err
	}

	return nil
}
