package logging

import (
	"io"
	"log"
	"os"
	"strings"
)

// Setting loggers for package main
// Check this utilization: https://www.honeybadger.io/blog/golang-logging/
var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	DebugLogger   *log.Logger
	ErrorLogger   *log.Logger
)

func init() {
	DebugLogger = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
	InfoLogger = log.New(os.Stdout, "INFO: ", log.LstdFlags)
	WarningLogger = log.New(os.Stdout, "WARN: ", log.LstdFlags|log.Lshortfile)
	ErrorLogger = log.New(os.Stdout, "ERROR: ", log.LstdFlags|log.Lshortfile)
}

// Setup happens in init function
// This function discards the logger writer based on log-level set
func Initialize(level string) {
	switch strings.ToLower(level) {
	case "info":
		DebugLogger = log.New(io.Discard, "DEBUG: ", log.LstdFlags)
	case "warning":
		DebugLogger = log.New(io.Discard, "DEBUG: ", log.LstdFlags)
		InfoLogger = log.New(io.Discard, "INFO: ", log.LstdFlags)
	case "error":
		DebugLogger = log.New(io.Discard, "DEBUG: ", log.LstdFlags)
		InfoLogger = log.New(io.Discard, "INFO: ", log.LstdFlags)
		WarningLogger = log.New(io.Discard, "WARN: ", log.LstdFlags|log.Lshortfile)
	}
}
