package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const MOM_PORT = 5672

type AggregationConfig struct {
	Id                int
	MomHost           string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

func loadConfig() (*AggregationConfig, error) {

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

	topSize, err := strconv.Atoi(os.Getenv("TOP_SIZE"))
	if err != nil {
		return nil, err
	}

	aggregationConfig := AggregationConfig{
		Id:                id,
		MomHost:           os.Getenv("MOM_HOST"),
		OutputQueue:       os.Getenv("OUTPUT_QUEUE"),
		SumAmount:         sumAmount,
		SumPrefix:         os.Getenv("SUM_PREFIX"),
		AggregationAmount: aggregationAmount,
		AggregationPrefix: os.Getenv("AGGREGATION_PREFIX"),
		TopSize:           topSize,
	}

	return &aggregationConfig, nil
}

func buildFruitTop(topSize int, fruitItemMap *map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItemMapKeys := make([]string, 0)
	for key := range *fruitItemMap {
		fruitItemMapKeys = append(fruitItemMapKeys, key)
	}
	sort.SliceStable(fruitItemMapKeys, func(i, j int) bool {
		fruitItemA := (*fruitItemMap)[fruitItemMapKeys[i]]
		fruitItemB := (*fruitItemMap)[fruitItemMapKeys[j]]
		return fruitItemB.Less(fruitItemA)
	})

	finalTopSize := min(topSize, len(fruitItemMapKeys))
	fruitTopRecords := make([]fruititem.FruitItem, finalTopSize)
	for i := range finalTopSize {
		fruitTopRecords[i] = (*fruitItemMap)[fruitItemMapKeys[i]]
	}

	return fruitTopRecords
}

func handleMessage(topSize int, fruitItemMap *map[string]fruititem.FruitItem, outputQueue middleware.Middleware, msg middleware.Message, ack func(), nack func()) {
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
		fruitTopRecords := buildFruitTop(topSize, fruitItemMap)
		message, err := inner.SerializeMessage(fruitTopRecords)
		if err != nil {
			log.Println("While serializing message", err)
			return
		}
		if err := outputQueue.Send(*message); err != nil {
			log.Println("While sending message", err)
			return
		}

		eofMessage := []fruititem.FruitItem{}
		message, err = inner.SerializeMessage(eofMessage)
		if err != nil {
			log.Println("While serializing message", err)
			return
		}
		if err := outputQueue.Send(*message); err != nil {
			log.Println("While sending message", err)
			return
		}
	}
}

func main() {
	var fruitItemMap = map[string]fruititem.FruitItem{}
	aggregationConfig, err := loadConfig()
	if err != nil {
		log.Println("While reading config", err)
		return
	}
	connectionConfig := middleware.ConnSettings{Hostname: aggregationConfig.MomHost, Port: MOM_PORT}
	outputQueue, err := middleware.CreateQueueMiddleware(aggregationConfig.OutputQueue, connectionConfig)
	if err != nil {
		log.Println("While creating input queue", err)
		return
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", aggregationConfig.AggregationPrefix, aggregationConfig.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(aggregationConfig.AggregationPrefix, inputExchangeRoutingKey, connectionConfig)
	if err != nil {
		log.Println("While creating input queue", err)
		return
	}

	inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		handleMessage(aggregationConfig.TopSize, &fruitItemMap, outputQueue, msg, ack, nack)
	})
}
