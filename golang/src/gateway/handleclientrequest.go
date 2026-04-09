package main

import (
	"log"
	"os"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func handleFruitRecordMessage(clientState ClientState, inputQueue middleware.Middleware) error {
	fruitRecord, err := external.ReadFruitRecord(clientState.ClientConnection)
	if err != nil {
		log.Println("While writting FRUIT_TOP", err)
		return err
	}

	message, err := clientState.MessageHandler.SerializeDataMessage(*fruitRecord)
	if err != nil {
		log.Println("While serializing data message", err)
		return err
	}
	if err := inputQueue.Send(*message); err != nil {
		log.Println("While sending data message")
		return err
	}
	if err := external.WriteAck(clientState.ClientConnection); err != nil {
		log.Println("While writing ACK message")
		return err
	}

	return nil
}

func handleEndOfRecordsMessage(clientState ClientState, inputQueue middleware.Middleware) error {
	log.Println("Recv end of records")
	message, err := clientState.MessageHandler.SerializeEofMessage()
	if err != nil {
		log.Println("While serializing eof message", err)
		return err
	}
	if err := inputQueue.Send(*message); err != nil {
		log.Println("While sending eof message")
		return err
	}

	if err := external.WriteAck(clientState.ClientConnection); err != nil {
		log.Println("While writing ACK message")
		return err
	}

	return nil
}

func handleClientRequest(clientState ClientState) {
	inputQueue, err := createQueue(os.Getenv("OUTPUT_QUEUE"))
	defer inputQueue.Close()

	if err != nil {
		log.Println("While readding rabbitmq host and ports", err)
		return
	}

loop:
	for {
		msgType, err := external.ReadMsgType(clientState.ClientConnection)
		if err != nil {
			log.Println("While reading message type", err)
			return
		}

		switch msgType {

		case external.FruitRecord:
			if err := handleFruitRecordMessage(clientState, inputQueue); err != nil {
				log.Println("While handling record message", err)
				return
			}

		case external.EndOfRecords:
			if err := handleEndOfRecordsMessage(clientState, inputQueue); err != nil {
				log.Println("While handling end of records message", err)
				return
			}
			break loop

		default:
			log.Println("Error: Read unexpected message type")
			return
		}
	}
}
