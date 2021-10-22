package broker

import (
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/nsqio/go-nsq"
)

// Tests the brokerless communication functionality through a client.
func TestClientPubSub(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	// Create a broker to simulate a remote machine
	datadir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Failed to create temporary datadir: %v", err)
	}
	defer os.RemoveAll(datadir)

	broker, err := New(&Config{
		Name:     "test-broker",
		Datadir:  datadir,
		Secret:   "secret test seed",
		Listener: &net.TCPAddr{IP: net.IPv4zero, Port: 0},
		Logger:   log.New("host", "broker"),
	})
	if err != nil {
		t.Fatalf("Failed to start message broker: %v", err)
	}
	defer broker.Close()

	brokerAddr := "127.0.0.1:" + strconv.Itoa(broker.Port())

	// Create a client to simulate a local machine
	client := NewClient("secret test seed", log.New("host", "client"))

	// Create a consumer with both broker and client to cross-check each other
	clientSub, err := client.NewConsumer("client-topic", "client")
	if err != nil {
		t.Fatalf("Failed to create client consumer: %v", err)
	}
	defer clientSub.Stop()

	brokerSub, err := broker.NewConsumer("broker-topic")
	if err != nil {
		t.Fatalf("Failed to create broker consumer: %v", err)
	}
	defer brokerSub.Stop()

	clientMailbox := make(chan string)
	clientSub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		clientMailbox <- string(message.Body)
		return nil
	}))
	brokerMailbox := make(chan string)
	brokerSub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		brokerMailbox <- string(message.Body)
		return nil
	}))

	if err := clientSub.ConnectToNSQD(brokerAddr); err != nil {
		t.Fatalf("Failed to connect client consumer to broker: %v", err)
	}
	if err := brokerSub.ConnectToNSQD(brokerAddr); err != nil {
		t.Fatalf("Failed to connect broker consumer to broker: %v", err)
	}
	// Create a producer with both broker and client to cross-check each other
	clientPub, err := client.NewProducer(brokerAddr)
	if err != nil {
		t.Fatalf("Failed to create client consumer: %v", err)
	}
	defer clientPub.Stop()

	brokerPub, err := broker.NewProducer(brokerAddr)
	if err != nil {
		t.Fatalf("Failed to create broker consumer: %v", err)
	}
	defer brokerPub.Stop()

	// Cross-send messages and verify that the consumers get them
	testPublishConsume(t, clientPub, "client-topic", "client->client", clientMailbox)
	testPublishConsume(t, brokerPub, "broker-topic", "broker->broker", brokerMailbox)
	testPublishConsume(t, clientPub, "broker-topic", "client->broker", brokerMailbox)
	testPublishConsume(t, brokerPub, "client-topic", "broker->client", clientMailbox)
}

// testPublishConsume is a helper to publish a message into a topic via a producer
// and ensure a consumer receives it and streams it into the given sink channel.
func testPublishConsume(t *testing.T, pub *nsq.Producer, topic string, message string, sub chan string) {
	t.Helper()

	if err := pub.Publish(topic, []byte(message)); err != nil {
		t.Errorf("Failed to publish %s: %v", message, err)
	} else {
		select {
		case received := <-sub:
			if received != message {
				t.Errorf("Consumed message mismatch: have %s, want %s", received, message)
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
}
