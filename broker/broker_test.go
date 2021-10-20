package broker

import (
	"io/ioutil"
	"os"
	"testing"
)

// Tests that the broker can be started and torn down.
func TestBrokerLifecycle(t *testing.T) {
	datadir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("Failed to create temporary datadir: %v", err)
	}
	defer os.RemoveAll(datadir)

	broker, err := New(&Config{
		Name:       "test-broker",
		Datadir:    datadir,
		Passphrase: "secret test seed",
		Port:       0,
	})
	if err != nil {
		t.Fatalf("Failed to start message broker: %v", err)
	}
	if err := broker.Close(); err != nil {
		t.Fatalf("Failed to stop message broker: %v", err)
	}
}
