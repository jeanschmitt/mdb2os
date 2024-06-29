package mdb2os

type Logger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type NoopLogger struct{}

func (NoopLogger) Info(_ string)  {}
func (NoopLogger) Warn(_ string)  {}
func (NoopLogger) Error(_ string) {}
