package main

import (
	"context"
	"time"

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

func (client *MongoClient) disconnect() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client.Disconnect(ctx)
}

func (client *MongoClient) insertDocToCollection(database string, collection string, doc interface{}) error {
	db := client.Database(database)
	col := db.Collection(collection)

	_, err := col.InsertOne(context.Background(), doc)

	if err != nil {
		return err
	}

	return nil
}
