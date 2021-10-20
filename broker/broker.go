// Package broker is a simplification wrapper around the NSQ message broker.
package broker

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/log"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/nsqd"
)

// Config is the set of options to fine tune the message broker.
type Config struct {
	Name     string       // Globally unique name for the broker instance
	Datadir  string       // Data directory to store NSQ related data
	Secret   string       // Shared secret to authenticate into te NSQ cluster
	Listener *net.TCPAddr // Listener address to for NSQ connections

	Logger log.Logger // Logger to allow differentiating brokers if many is embedded
}

// Broker is a locally running message broker that transmits messages between a
// local node (consensus or execution) and remote ones via NSQ.
//
// Internally, each broker runs its own NSQ broker and will maintain a consumer
// status to **all** other known NSQ brokers to exchange node, topic and channel
// topology information.
type Broker struct {
	name string // Globally unique name for the broker, used by consumer channels

	tlsCert []byte // Certificate to use for authenticating to other brokers
	tlsKey  []byte // Private key to use for encrypting traffic with other brokers

	daemon *nsqd.NSQD // Message broker embedded in this process
	logger log.Logger // Logger to allow differentiating brokers if many is embedded
}

// New constructs an NSQ broker to communicate with other nodes through.
func New(config *Config) (*Broker, error) {
	// Make sure the config is valid
	if !nsq.IsValidChannelName(config.Name) {
		return nil, fmt.Errorf("invalid broker name '%s', must be alphanumeric", config.Name)
	}
	logger := config.Logger
	if logger == nil {
		logger = log.New()
	}
	logger.Info("Starting message broker", "name", config.Name, "datadir", config.Datadir, "bind", config.Listener)

	// Configure a new NSQ message broker to act as the multiplexer
	opts := nsqd.NewOptions()
	opts.DataPath = config.Datadir

	if config.Listener != nil {
		opts.TCPAddress = config.Listener.String()
	} else {
		opts.TCPAddress = "0.0.0.0:0" // Default to a random port, public routing
	}
	opts.HTTPAddress = ""  // Disable the HTTP interface
	opts.HTTPSAddress = "" // Disable the HTTPS interface

	opts.LogLevel = nsqd.LOG_DEBUG    // We'd like to receive all the broker messages
	opts.Logger = &nsqdLogger{logger} // Replace the default stderr logger with ours

	// Create an ephemeral key on disk and delete as soon as it's loaded. It's
	// needed to work around the NSQ library limitation.
	cert, key := makeTLSCert(config.Secret)
	os.MkdirAll(config.Datadir, 0700)

	if err := ioutil.WriteFile(filepath.Join(config.Datadir, "secret.cert"), cert, 0600); err != nil {
		return nil, err
	}
	defer os.Remove(filepath.Join(config.Datadir, "secret.cert"))

	if err := ioutil.WriteFile(filepath.Join(config.Datadir, "secret.key"), key, 0600); err != nil {
		return nil, err
	}
	defer os.Remove(filepath.Join(config.Datadir, "secret.key"))

	opts.TLSRootCAFile = filepath.Join(config.Datadir, "secret.cert")
	opts.TLSCert = filepath.Join(config.Datadir, "secret.cert")
	opts.TLSKey = filepath.Join(config.Datadir, "secret.key")

	opts.TLSRequired = nsqd.TLSRequired         // Enable TLS encryption for all traffic
	opts.TLSClientAuthPolicy = "require-verify" // Require TLS authentication from all clients
	opts.TLSMinVersion = tls.VersionTLS12       // Require the newest supported TLS protocol  // TODO(karalabe): Fix after https://github.com/nsqio/nsq/pull/1385

	// Create the NSQ daemon and return it without booting up
	daemon, err := nsqd.New(opts)
	if err != nil {
		return nil, err
	}
	go daemon.Main()

	return &Broker{
		name:    config.Name,
		tlsCert: cert,
		tlsKey:  key,
		daemon:  daemon,
		logger:  logger,
	}, nil
}

// Close terminates the NSQ daemon.
func (b *Broker) Close() error {
	b.daemon.Exit()
	return nil
}

// Name returns the globally unique (user assigned) name of the broker.
func (b *Broker) Name() string {
	return b.name
}

// Port returns the local port number the broker is listening on.
func (b *Broker) Port() int {
	return b.daemon.RealTCPAddr().Port
}

// NewProducer creates a new producer connected to the specified remote (or local)
// NSQD daemon instance.
func (b *Broker) NewProducer(addr string) (*nsq.Producer, error) {
	config := nsq.NewConfig()
	config.Snappy = true
	config.TlsV1 = true
	config.TlsConfig = makeTLSConfig(b.tlsCert, b.tlsKey)

	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err
	}
	producer.SetLogger(&nsqProducerLogger{b.logger}, nsq.LogLevelDebug)

	return producer, nil
}

// NewConsumer creates a new consumer configured to authenticate into the broker
// cluster and to listen for specific events; though the connectivity itself is
// left for the outside caller.
func (b *Broker) NewConsumer(topic string) (*nsq.Consumer, error) {
	config := nsq.NewConfig()
	config.Snappy = true
	config.TlsV1 = true
	config.TlsConfig = makeTLSConfig(b.tlsCert, b.tlsKey)

	consumer, err := nsq.NewConsumer(topic, b.name, config)
	if err != nil {
		return nil, err
	}
	consumer.SetLogger(&nsqConsumerLogger{b.logger}, nsq.LogLevelDebug)

	return consumer, nil
}
