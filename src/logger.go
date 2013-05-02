package main

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"strings"
)

type logRequest struct {
	record []string
	done   chan int
}

type logger struct {
	file      *os.File
	csvWriter *csv.Writer
	requests  chan *logRequest
}

func newLogger(logFilePath string) *logger {
	err := os.MkdirAll(path.Dir(logFilePath), 0)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE, 0)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}

	l := &logger{file, csv.NewWriter(file), make(chan *logRequest)}

	go l.loggingLoop()

	return l
}

func (l *logger) loggingLoop() {
	for {
		req := <-l.requests
		err := l.csvWriter.Write(req.record)
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}

		l.csvWriter.Flush()
		err = l.file.Sync()
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}
		req.done <- 1
	}
}

func (l *logger) write(txId string, state TxState, args ...string) {
	record := []string{txId, state.String()}
	record = append(record, args...)
	log.Println(record)
	done := make(chan int)
	l.requests <- &logRequest{record, done}
	<-done
}

type ConditionalWriter struct{}

func NewConditionalWriter() *ConditionalWriter {
	return &ConditionalWriter{}
}

func (w *ConditionalWriter) Write(b []byte) (n int, err error) {
	if !strings.Contains(string(b), "The specified network name is no longer available") {
		n, err = os.Stdout.Write(b)
	}
	return
}
