package main

import (
	"log"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func handleClientResponse(outputQueue middleware.Middleware, msg middleware.Message, ack func(), nack func()) {
	sharedClientStates.Lock.Lock()
	defer sharedClientStates.Lock.Unlock()

	clientIndex := 0
	for _, clientState := range sharedClientStates.ClientStates {
		log.Println("Reading from result queue")

		fruitTop, err := clientState.MessageHandler.DeserializeResultMessage(&msg)
		if err != nil {
			log.Println("While reading from output queue", err)
			nack()
			outputQueue.StopConsuming()
			return
		}

		// Not a message for this client
		if fruitTop == nil {
			clientIndex++
			continue
		}

		if err := external.WriteFruitTop(clientState.ClientConnection, fruitTop); err != nil {
			log.Println("While writing FRUIT_TOP message", err)
			break
		}

		msgType, err := external.ReadMsgType(clientState.ClientConnection)
		if err != nil {
			log.Println("While reading message type", err)
			break
		}
		if msgType != external.Ack {
			log.Println("Expected ACK message")
			break
		}
		break
	}

	ack()
	sharedClientStates.ClientStates = append(sharedClientStates.ClientStates[:clientIndex], sharedClientStates.ClientStates[clientIndex+1:]...)
}
