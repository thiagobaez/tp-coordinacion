package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/external"
)

const CONNECTION_ATTEMPS = 3
const CONNECTION_ATTEMPS_DELAY_MS = 300

type ConnectionConfig struct {
	host    string
	port    string
	attemps int
	delayMs int
}

func loadConnectionConfig() (*ConnectionConfig, error) {
	connectionConfig := ConnectionConfig{
		host:    os.Getenv("SERVER_HOST"),
		port:    os.Getenv("SERVER_PORT"),
		attemps: CONNECTION_ATTEMPS,
		delayMs: CONNECTION_ATTEMPS_DELAY_MS,
	}
	return &connectionConfig, nil
}

type FileConfig struct {
	input  string
	output string
}

func loadFileConfig() (*FileConfig, error) {
	fileConfig := FileConfig{
		input:  os.Getenv("INPUT_FILE"),
		output: os.Getenv("OUTPUT_FILE"),
	}
	return &fileConfig, nil
}

func connectToServer(connectionConfig *ConnectionConfig) (net.Conn, error) {
	var err error
	var serverConnection net.Conn

	for range connectionConfig.attemps {
		serverConnection, err = net.Dial("tcp", connectionConfig.host+":"+connectionConfig.port)
		if err != nil {
			log.Println("Retrying connection...")
			time.Sleep(time.Duration(connectionConfig.delayMs) * time.Millisecond)
			continue
		}

		break
	}

	if err != nil {
		return nil, err
	}

	return serverConnection, nil
}

func handleSignals(serverConnection net.Conn, open *bool) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("SIGTERM signal received")
	*open = false
	serverConnection.Close()
}

func sendFruitRecords(serverConnection net.Conn, fileConfig *FileConfig) error {
	file, err := os.Open(fileConfig.input)
	if err != nil {
		log.Println("Error while reading the file", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := string(scanner.Text())
		columns := strings.Split(line, ",")
		fruit := columns[0]
		amount, err := strconv.ParseInt(columns[1], 10, 32)
		if err != nil {
			log.Println("Error while reading fruit record", err)
			return err
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}
		if err := external.WriteFruitRecord(serverConnection, &fruitRecord); err != nil {
			return err
		}

		msgType, err := external.ReadMsgType(serverConnection)
		if err != nil {
			log.Println("Error while reading ack message", err)
			return err
		}
		if msgType != external.Ack {
			return errors.New("Expected Ack message")
		}
	}

	if err := external.WriteEndOfRecords(serverConnection); err != nil {
		return err
	}
	msgType, err := external.ReadMsgType(serverConnection)
	if err != nil {
		log.Println("Error while reading ack message", err)
		return err
	}
	if msgType != external.Ack {
		return errors.New("Expected Ack message")
	}

	return nil
}

func recvFruitTop(serverConnection net.Conn, fileConfig *FileConfig) error {

	msgType, err := external.ReadMsgType(serverConnection)
	if err != nil {
		log.Println("Error while reading message type", err)
		return err
	}
	if msgType != external.FruitTop {
		return errors.New("Expected FruitTop message")
	}

	fruitTop, err := external.ReadFruitTop(serverConnection)
	if err != nil {
		log.Println("Error while reading FruitTop message", err)
		return err
	}
	if err := external.WriteAck(serverConnection); err != nil {
		log.Println("Error while writing ack message", err)
		return err
	}

	outputFile, err := os.Create(fileConfig.output)
	if err != nil {
		log.Println("Error while creating output file", err)
		return err
	}
	outputFileWriter := csv.NewWriter(outputFile)

	for _, fruitRecord := range fruitTop {
		line := []string{fruitRecord.Fruit, strconv.Itoa(int(fruitRecord.Amount))}
		err := outputFileWriter.Write(line)
		if err != nil {
			log.Println("Error while writing output file", err)
			return err
		}
	}
	outputFileWriter.Flush()

	return nil
}

func main() {
	var open bool = true

	connectionConfig, err := loadConnectionConfig()
	if err != nil {
		log.Println("Error while reading connection environment variables:", err)
		return
	}
	fileConfig, err := loadFileConfig()
	if err != nil {
		log.Println("Error while reading file environment variables:", err)
		return
	}

	serverConnection, err := connectToServer(connectionConfig)
	if err != nil {
		log.Println("Error while connecting to server:", err)
		return
	}
	defer serverConnection.Close()
	go handleSignals(serverConnection, &open)

	if err := sendFruitRecords(serverConnection, fileConfig); err != nil && open {
		log.Println("Error while sending fruit records:", err)
		return
	}

	if err := recvFruitTop(serverConnection, fileConfig); err != nil && open {
		log.Println("Error while receiving fruit top:", err)
		return
	}
}
