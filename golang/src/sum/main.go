package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const MOM_PORT = 5672

type SumConfig struct {
	Id                int
	MomHost           string
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

func loadConfig() (*SumConfig, error) {

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return nil, err
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return nil, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return nil, err
	}

	connectionConfig := SumConfig{
		Id:                id,
		MomHost:           os.Getenv("MOM_HOST"),
		InputQueue:        os.Getenv("INPUT_QUEUE"),
		SumAmount:         sumAmount,
		SumPrefix:         os.Getenv("SUM_PREFIX"),
		AggregationAmount: aggregationAmount,
		AggregationPrefix: os.Getenv("AGGREGATION_PREFIX"),
	}

	return &connectionConfig, nil
}

func handleMessage(fruitItemMap *map[string]fruititem.FruitItem, outputExchange middleware.Middleware, msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, err := inner.DeserializeMessage(&msg)
	if err != nil {
		log.Println("While deserializing message", err)
		return
	}

	if len(fruitRecords) > 0 {
		for _, fruitRecord := range fruitRecords {
			_, ok := (*fruitItemMap)[fruitRecord.Fruit]
			if ok {
				(*fruitItemMap)[fruitRecord.Fruit] = (*fruitItemMap)[fruitRecord.Fruit].Sum(fruitRecord)
			} else {
				(*fruitItemMap)[fruitRecord.Fruit] = fruitRecord
			}
		}

	} else {
		log.Println("Received end of records message")
		for key := range *fruitItemMap {
			fruitRecord := []fruititem.FruitItem{(*fruitItemMap)[key]}
			message, err := inner.SerializeMessage(fruitRecord)
			if err != nil {
				log.Println("While serializing message", err)
				return
			}
			if err := outputExchange.Send(*message); err != nil {
				log.Println("While sending message", err)
				return
			}
		}

		eofMessage := []fruititem.FruitItem{}
		message, err := inner.SerializeMessage(eofMessage)
		if err != nil {
			log.Println("While serializing message", err)
			return
		}
		if err := outputExchange.Send(*message); err != nil {
			log.Println("While sending message", err)
			return
		}
	}
}

func main() {
	var fruitItemMap = map[string]fruititem.FruitItem{}
	sumConfig, err := loadConfig()
	if err != nil {
		log.Println("While reading config", err)
		return
	}
	connectionConfig := middleware.ConnSettings{Hostname: sumConfig.MomHost, Port: MOM_PORT}
	inputQueue, err := middleware.CreateQueueMiddleware(sumConfig.InputQueue, connectionConfig)
	if err != nil {
		log.Println("While creating input queue", err)
		return
	}

	outputExchangeRoutekeys := []string{}
	for i := range sumConfig.AggregationAmount {
		outputExchange := fmt.Sprintf("%s_%d", sumConfig.AggregationPrefix, i)
		log.Println(outputExchange)
		outputExchangeRoutekeys = append(outputExchangeRoutekeys, outputExchange)
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(sumConfig.AggregationPrefix, outputExchangeRoutekeys, connectionConfig)
	if err != nil {
		log.Println("While creating input queue", err)
		return
	}

	inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		handleMessage(&fruitItemMap, outputExchange, msg, ack, nack)
	})
}
