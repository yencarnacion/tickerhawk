package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

type Logger struct {
	mu    sync.Mutex
	level LogLevel
	std   *log.Logger
}

func NewLogger(level string) *Logger {
	lv := LevelInfo
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lv = LevelDebug
	case "info":
		lv = LevelInfo
	case "warn", "warning":
		lv = LevelWarn
	case "error":
		lv = LevelError
	}
	return &Logger{
		level: lv,
		std:   log.New(os.Stdout, "", 0),
	}
}

func (l *Logger) Debugf(format string, args ...any) { l.printf(LevelDebug, "DEBUG", format, args...) }
func (l *Logger) Infof(format string, args ...any)  { l.printf(LevelInfo, "INFO ", format, args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.printf(LevelWarn, "WARN ", format, args...) }
func (l *Logger) Errorf(format string, args ...any) { l.printf(LevelError, "ERROR", format, args...) }

func (l *Logger) printf(lv LogLevel, tag, format string, args ...any) {
	if lv < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	l.std.Printf("%s [%s] %s", ts, tag, msg)
}
