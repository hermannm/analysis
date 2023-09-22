package log

import (
	"fmt"
	"log/slog"

	"hermannm.dev/wrap"
)

func Info(msg string) {
	slog.Info(msg)
}

func Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args...))
}

func Warn(msg string) {
	slog.Warn(msg)
}

func Warnf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args...))
}

func Error(err error, msg string) {
	if msg != "" {
		err = wrap.Error(err, msg)
	}

	slog.Error(err.Error())
}

func Errorf(err error, format string, args ...any) {
	slog.Error(wrap.Errorf(err, format, args...).Error())
}
