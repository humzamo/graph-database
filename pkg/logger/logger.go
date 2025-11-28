package logger

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type contextKey string

var contextKeyLogger contextKey = "logger"

type LoggableService interface {
	Name() string
	AppEnvironment() string
	AppVersion() string
	IsDebug() bool
}

type loggerKey = string

const (
	keyService     loggerKey = "service"
	keyAppVersion  loggerKey = "appVersion"
	keyEnvironment loggerKey = "environment"
	keyFunction    loggerKey = "function"
)

func NewLogger(ctx context.Context, service LoggableService, functionName string) *zerolog.Logger {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if service.IsDebug() {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	loggerWithFields := log.With().Stack().Caller().
		Str(keyService, service.Name()).
		Str(keyAppVersion, service.AppVersion()).
		Str(keyEnvironment, service.AppEnvironment()).
		Str(keyFunction, functionName).
		Logger()
	return &loggerWithFields
}

func LogError(s *zerolog.Logger, err error) {
	s.Error().Err(err).Msg(err.Error())
}

func NewBaseLogger(ctx context.Context, serviceName, functionName string) *zerolog.Logger {
	loggerWithFields := log.With().Stack().Caller().
		Str(keyService, serviceName).
		Str(keyFunction, functionName).
		Logger()
	return &loggerWithFields
}

func NewRawLogger() *zerolog.Logger {
	l := log.With().Stack().Caller().Logger()
	return &l
}

func FromContext(ctx context.Context) *zerolog.Logger {
	if l, ok := ctx.Value(contextKeyLogger).(*zerolog.Logger); ok {
		return l
	}
	return NewRawLogger()
}

func WithContext(ctx context.Context, l *zerolog.Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, l)
}
