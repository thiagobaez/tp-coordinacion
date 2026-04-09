package main

import (
	"errors"
	"log"
	"os"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func createQueue(queueName string) (middleware.Middleware, error) {
	if queueName == "" {
		return nil, errors.New("Empty queue name string")
	}
	hostName := os.Getenv("MOM_HOST")
	if queueName == "" {
		return nil, errors.New("Empty mom host name string")
	}
	connectionConfig := middleware.ConnSettings{Hostname: hostName, Port: 5672}
	inputQueue, err := middleware.CreateQueueMiddleware(queueName, connectionConfig)
	if err != nil {
		log.Println("While creating queue", err)
		return nil, err
	}

	return inputQueue, nil
}
