package log

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"hermannm.dev/wrap"
)

func Info(msg string) {
	log(slog.LevelInfo, msg)
}

func Infof(format string, args ...any) {
	log(slog.LevelInfo, fmt.Sprintf(format, args...))
}

func Warn(msg string) {
	log(slog.LevelWarn, msg)
}

func Warnf(format string, args ...any) {
	log(slog.LevelWarn, fmt.Sprintf(format, args...))
}

func Error(err error, msg string) {
	if err == nil {
		log(slog.LevelError, msg)
	} else {
		if msg != "" {
			err = wrap.Error(err, msg)
		}

		log(slog.LevelError, err.Error())
	}
}

func Errorf(err error, format string, args ...any) {
	if err == nil {
		log(slog.LevelError, fmt.Sprintf(format, args...))
	} else {
		log(slog.LevelError, wrap.Errorf(err, format, args...).Error())
	}
}

func log(level slog.Level, msg string) {
	logger := slog.Default()
	if !logger.Enabled(context.Background(), level) {
		return
	}

	// Follows the example from the slog package of how to properly wrap its functions:
	// https://pkg.go.dev/golang.org/x/exp/slog#hdr-Wrapping_output_methods
	var callers [1]uintptr
	// Skips 3, because we want to skip:
	// - the call to Callers
	// - the call to log (this function)
	// - the call to the public log function that uses this function
	runtime.Callers(3, callers[:])

	record := slog.NewRecord(time.Now(), level, msg, callers[0])
	_ = logger.Handler().Handle(context.Background(), record)
}
