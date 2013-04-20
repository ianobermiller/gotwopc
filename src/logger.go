package main

import (
	"fmt"
	"log"
	"os"
)

type logger struct {
	file *os.File
}

func newLogger(logFilePath string) *logger {
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE, 0)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}
	return &logger{file}
}

func (l *logger) write(args ...interface{}) {
	_, err := l.file.WriteString(fmt.Sprintln(args...))
	if err != nil {
		log.Fatalln("logger.log:", err)
	}

	err = l.file.Sync()
	if err != nil {
		log.Fatalln("logger.log:", err)
	}
}
