package main

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"strings"
)

type logEntry struct {
	txId  string
	state TxState
	op    Operation
	key   string
}

type logRequest struct {
	record []string
	done   chan int
}

type logger struct {
	path      string
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

	l := &logger{logFilePath, file, csv.NewWriter(file), make(chan *logRequest)}

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

func (l *logger) writeSpecial(directive string) {
	l.writeOp(directive, NoState, NoOp, "")
}

func (l *logger) writeState(txId string, state TxState) {
	l.writeOp(txId, state, NoOp, "")
}

func (l *logger) writeOp(txId string, state TxState, op Operation, key string) {
	record := []string{txId, state.String(), op.String(), key}
	done := make(chan int)
	l.requests <- &logRequest{record, done}
	<-done
}

func (l *logger) read() (entries []logEntry, err error) {
	entries = make([]logEntry, 0)
	file, err := os.OpenFile(l.path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return
	}

	for _, record := range records {
		entries = append(entries, logEntry{record[0], ParseTxState(record[1]), ParseOperation(record[2]), record[3]})
	}
	return
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
