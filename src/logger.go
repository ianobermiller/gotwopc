package main

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"strings"
)

type logger struct {
	file      *os.File
	csvWriter *csv.Writer
}

func newLogger(logFilePath string) *logger {
	err := os.MkdirAll(path.Dir(logFilePath), 0)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE, 0)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}
	return &logger{file, csv.NewWriter(file)}
}

func (l *logger) write(txId string, state TxState, args ...string) {
	record := []string{txId, state.String()}
	record = append(record, args...)
	err := l.csvWriter.Write(record)
	if err != nil {
		log.Fatalln("logger.write:", err)
	}

	l.csvWriter.Flush()
	err = l.file.Sync()
	if err != nil {
		log.Fatalln("logger.log:", err)
	}
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
