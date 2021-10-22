package broker

import (
	"github.com/ethereum/go-ethereum/log"
	"github.com/nsqio/go-nsq"
)

// Client is an interface through which NSQ messages can be produced and consumed
// against a remote broker, without running an actual broker. The primary use case
// is to tap into an existing broker cluster without affecting the network topology
// (e.g. to send control commands).
type Client struct {
	tlsCert []byte // Certificate to use for authenticating to other brokers
	tlsKey  []byte // Private key to use for encrypting traffic with other brokers

	logger log.Logger // Logger to allow differentiating clients if many is embedded
}

// NewClient creates a communication interface without a local broker attached.
func NewClient(secret string, logger log.Logger) *Client {
	cert, key := makeTLSCert(secret)
	if logger == nil {
		logger = log.New()
	}
	return &Client{
		tlsCert: cert,
		tlsKey:  key,
		logger:  logger,
	}
}

// NewProducer creates a new producer connected to the specified remote NSQD
// daemon instance.
func (c *Client) NewProducer(addr string) (*nsq.Producer, error) {
	config := nsq.NewConfig()
	config.Snappy = true
	config.TlsV1 = true
	config.TlsConfig = makeTLSConfig(c.tlsCert, c.tlsKey)

	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}
	producer.SetLogger(&nsqProducerLogger{c.logger}, nsq.LogLevelDebug)

	return producer, nil
}

// NewConsumer creates a new consumer configured to authenticate into the broker
// cluster and to listen for specific events; though the connectivity itself is
// left for the outside caller.
func (c *Client) NewConsumer(topic string, channel string) (*nsq.Consumer, error) {
	config := nsq.NewConfig()
	config.Snappy = true
	config.TlsV1 = true
	config.TlsConfig = makeTLSConfig(c.tlsCert, c.tlsKey)

	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, err
	}
	consumer.SetLogger(&nsqConsumerLogger{c.logger}, nsq.LogLevelDebug)

	return consumer, nil
}
