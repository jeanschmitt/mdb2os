package mdb2os

type Logger interface {
	Info(msg string, args ...any)
}

type NoopLogger struct{}

func (NoopLogger) Info(msg string, args ...any) {}
