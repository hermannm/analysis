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
	if err == nil {
		slog.Error(msg)
	} else {
		if msg != "" {
			err = wrap.Error(err, msg)
		}

		slog.Error(err.Error())
	}
}

func Errorf(err error, format string, args ...any) {
	if err == nil {
		slog.Error(fmt.Sprintf(format, args...))
	} else {
		slog.Error(wrap.Errorf(err, format, args...).Error())
	}
}
