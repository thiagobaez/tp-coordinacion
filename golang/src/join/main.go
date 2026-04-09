package main

import (
	"log"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const MOM_PORT = 5672

type Join struct {
	MomHost           string
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

func loadConfig() (*Join, error) {
	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return nil, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return nil, err
	}

	topSize, err := strconv.Atoi(os.Getenv("TOP_SIZE"))
	if err != nil {
		return nil, err
	}

	joinConfig := Join{
		MomHost:           os.Getenv("MOM_HOST"),
		InputQueue:        os.Getenv("INPUT_QUEUE"),
		OutputQueue:       os.Getenv("OUTPUT_QUEUE"),
		SumAmount:         sumAmount,
		SumPrefix:         os.Getenv("SUM_PREFIX"),
		AggregationAmount: aggregationAmount,
		AggregationPrefix: os.Getenv("AGGREGATION_PREFIX"),
		TopSize:           topSize,
	}

	return &joinConfig, nil
}

func handleMessage(outputQueue middleware.Middleware, msg middleware.Message, ack func(), nack func()) {
	defer ack()
	if err := outputQueue.Send(msg); err != nil {
		log.Println("While sending top", err)
	}
}

func main() {
	joinConfig, err := loadConfig()
	if err != nil {
		log.Println("While reading config", err)
		return
	}
	connectionConfig := middleware.ConnSettings{Hostname: joinConfig.MomHost, Port: MOM_PORT}
	inputQueue, err := middleware.CreateQueueMiddleware(joinConfig.InputQueue, connectionConfig)
	if err != nil {
		log.Println("While creating input queue", err)
		return
	}
	outputQueue, err := middleware.CreateQueueMiddleware(joinConfig.OutputQueue, connectionConfig)
	if err != nil {
		log.Println("While creating output queue", err)
		return
	}

	inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		handleMessage(outputQueue, msg, ack, nack)
	})
}
