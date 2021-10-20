package broker

import (
	"strings"

	"github.com/ethereum/go-ethereum/log"
)

// nsqdLogger is a helper that wraps the log messages emitted by the NSQ daemon
// into log messages native to this project.
type nsqdLogger struct {
	logger log.Logger
}

// Output implements the lg.Logger interface used by NSQ.
func (l *nsqdLogger) Output(maxdepth int, s string) error {
	// Unpack the log context
	level := strings.Split(s, " ")[0]
	s = s[len(level)+1:]

	module := strings.Split(s, " ")[0]
	if len(module) > 0 && module[len(module)-1] == ':' {
		module, s = module[:len(module)-1], s[len(module)+1:]
	} else {
		module = "" // not a tagged log
	}
	// Create a contextual log and do proper logging
	var logger log.Logger
	if module == "" {
		logger = l.logger
	} else {
		logger = l.logger.New("module", strings.ToLower(module))
	}
	switch level {
	case "DEBUG:":
		logger.Trace("Broker server emitted log", "msg", s)
	case "INFO:":
		logger.Debug("Broker server emitted log", "msg", s)
	case "WARNING:":
		logger.Warn("Broker server emitted log", "msg", s)
	case "ERROR:":
		logger.Error("Broker server emitted log", "msg", s)
	default:
		logger.Error("Broker server emitted unknown log", "msg", s)
	}
	return nil
}

// nsqProducerLogger is a helper that wraps the log messages emitted by the NSQ
// client into log messages native to this project.
type nsqProducerLogger struct {
	logger log.Logger
}

// Output implements the lg.Logger interface used by NSQ.
func (l *nsqProducerLogger) Output(maxdepth int, s string) error {
	// Unpack the log context
	level := s[:3]
	s = strings.TrimSpace(s[3:])

	id := strings.Split(s, " ")[0]
	s = s[len(id)+1:]

	addr := strings.Trim(strings.Split(s, " ")[0], "()")
	s = s[len(addr)+2+1:]

	// Create a contextual log and do proper logging
	logger := l.logger.New("id", id, "nsqd", addr)

	switch level {
	case "DBG":
		logger.Trace("Broker producer emitted log", "msg", s)
	case "INF":
		logger.Debug("Broker producer emitted log", "msg", s)
	case "ERR":
		logger.Error("Broker producer emitted log", "msg", s)
	default:
		logger.Error("Broker producer emitted unknown log", "msg", s)
	}
	return nil
}

// nsqConsumerLogger is a helper that wraps the log messages emitted by the NSQ
// client into log messages native to this project.
type nsqConsumerLogger struct {
	logger log.Logger
}

// Output implements the lg.Logger interface used by NSQ.
func (l *nsqConsumerLogger) Output(maxdepth int, s string) error {
	// Unpack the log context
	level := s[:3]
	s = strings.TrimSpace(s[3:])

	id := strings.Split(s, " ")[0]
	s = s[len(id)+1:]

	sub := strings.Trim(strings.Split(s, " ")[0], "[]")
	s = s[len(sub)+2+1:]

	// Create a contextual log and do proper logging
	logger := l.logger.New("id", id, "sub", sub)

	switch level {
	case "DBG":
		logger.Trace("Broker consumer emitted log", "msg", s)
	case "INF":
		logger.Debug("Broker consumer emitted log", "msg", s)
	case "ERR":
		logger.Error("Broker consumer emitted log", "msg", s)
	default:
		logger.Error("Broker consumer emitted unknown log", "msg", s)
	}
	return nil
}
