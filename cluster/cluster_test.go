package cluster

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/karalabe/minority/broker"
)

// ipv4Localhost is the localhost address used by all tests.
var ipv4Localhost = net.IPv4(127, 0, 0, 1)

// Tests that the cluster can be started and torn down.
func TestClusterLifecycle(t *testing.T) {
	// Configure and create the gateway message broker
	datadir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadir)

	broker, err := broker.New(&broker.Config{
		Name:    "test-broker",
		Datadir: datadir,
		Secret:  "secret test seed",
	})
	defer broker.Close()

	// Wrap the broker into a cluster and check its lifecycle
	cluster, err := New(&Config{
		External: &net.TCPAddr{
			IP:   ipv4Localhost,
			Port: broker.Port(),
		},
	}, broker)
	if err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}
	if err := cluster.Close(); err != nil {
		t.Fatalf("Failed to stop broker cluster: %v", err)
	}
}

// Tests that a multi-node cluster can converge to the same view.
func TestClusterConvergence(t *testing.T) {
	// Create 3 brokers and cluster instances to simulate 3 machines
	datadirA, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirA)
	datadirB, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirB)
	datadirC, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirC)

	brokerA, _ := broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A")})
	defer brokerA.Close()
	brokerB, _ := broker.New(&broker.Config{Name: "test-broker-B", Datadir: datadirB, Logger: log.New("tester", "B")})
	defer brokerB.Close()
	brokerC, _ := broker.New(&broker.Config{Name: "test-broker-C", Datadir: datadirC, Logger: log.New("tester", "C")})
	defer brokerC.Close()

	clusterA, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A")}, brokerA)
	defer clusterA.Close()
	clusterB, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerB.Port()}, Logger: log.New("tester", "B")}, brokerB)
	defer clusterB.Close()
	clusterC, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerC.Port()}, Logger: log.New("tester", "C")}, brokerC)
	defer clusterC.Close()

	// Join together the clusters and check that the operations are accepted
	externalA, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	externalB, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerB.Port()))
	externalC, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerC.Port()))

	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A->B: %v", err)
	}
	if err := clusterB.Join(externalC); err != nil {
		t.Fatalf("Failed to join cluster B->C: %v", err)
	}
	// Wait a bit for the cluster to converge and check its status (would be nice
	// to do this without waiting)
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
			"test-broker-C": externalC.String(),
		},
	)
}

// Tests that stopping and restarting a node with a new name in a cluster will
// converge to the new name across all peers.
func TestRenameConvergence(t *testing.T) {
	// Create 3 brokers and cluster instances to simulate 3 machines
	datadirA, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirA)
	datadirB, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirB)
	datadirC, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirC)

	brokerA, _ := broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A")})
	defer brokerA.Close()
	brokerB, _ := broker.New(&broker.Config{Name: "test-broker-B", Datadir: datadirB, Logger: log.New("tester", "B")})
	defer brokerB.Close()
	brokerC, _ := broker.New(&broker.Config{Name: "test-broker-C", Datadir: datadirC, Logger: log.New("tester", "C")})
	defer brokerC.Close()

	clusterA, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A")}, brokerA)
	defer clusterA.Close()
	clusterB, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerB.Port()}, Logger: log.New("tester", "B")}, brokerB)
	defer clusterB.Close()
	clusterC, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerC.Port()}, Logger: log.New("tester", "C")}, brokerC)
	defer clusterC.Close()

	// Join together the clusters and check that the operations are accepted
	externalA, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	externalB, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerB.Port()))
	externalC, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerC.Port()))

	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A->B: %v", err)
	}
	if err := clusterB.Join(externalC); err != nil {
		t.Fatalf("Failed to join cluster B->C: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
			"test-broker-C": externalC.String(),
		},
	)
	// Terminate one of the clusters and restart with a new name
	clusterA.Close()
	brokerA.Close()

	brokerA, _ = broker.New(&broker.Config{Name: "test-broker-A2", Datadir: datadirA, Listener: externalA, Logger: log.New("tester", "A2")})
	defer brokerA.Close()

	clusterA, _ = New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A2")}, brokerA)
	defer clusterA.Close()

	// Since there's no event happening to detect cluster downtime, force a join
	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A'->B: %v", err)
	}
	// Wait a bit for the cluster to converge and check its status (would be nice
	// to do this without waiting)
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A2": externalA.String(),
			"test-broker-B":  externalB.String(),
			"test-broker-C":  externalC.String(),
		},
	)
}

// Tests that stopping and restarting a node with a new name in a cluster will
// converge to the new name across all peers, even if the node was previously
// deemed dead.
func TestOfflineRenameConvergence(t *testing.T) {
	// Create 3 brokers and cluster instances to simulate 3 machines
	datadirA, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirA)
	datadirB, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirB)
	datadirC, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirC)

	brokerA, _ := broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A")})
	defer brokerA.Close()
	brokerB, _ := broker.New(&broker.Config{Name: "test-broker-B", Datadir: datadirB, Logger: log.New("tester", "B")})
	defer brokerB.Close()
	brokerC, _ := broker.New(&broker.Config{Name: "test-broker-C", Datadir: datadirC, Logger: log.New("tester", "C")})
	defer brokerC.Close()

	clusterA, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A")}, brokerA)
	defer clusterA.Close()
	clusterB, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerB.Port()}, Logger: log.New("tester", "B")}, brokerB)
	defer clusterB.Close()
	clusterC, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerC.Port()}, Logger: log.New("tester", "C")}, brokerC)
	defer clusterC.Close()

	// Join together two clusters and then split them apart
	externalA, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	externalB, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerB.Port()))
	externalC, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerC.Port()))

	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A->B: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
		},
	)
	// Terminate one of the clusters and force a health check fail
	clusterA.Close()
	brokerA.Close()

	if err := clusterB.Join(externalC); err != nil {
		t.Fatalf("Failed to join cluster B->C: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	// TODO(karalabe): check partial convergence?

	// Restart the terminated cluster with a new name
	brokerA, _ = broker.New(&broker.Config{Name: "test-broker-A2", Datadir: datadirA, Listener: externalA, Logger: log.New("tester", "A2")})
	defer brokerA.Close()

	clusterA, _ = New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A2")}, brokerA)
	defer clusterA.Close()

	// Since there's no event happening to detect cluster downtime, force a join
	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A'->B: %v", err)
	}
	// Wait a bit for the cluster to converge and check its status (would be nice
	// to do this without waiting)
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A2": externalA.String(),
			"test-broker-B":  externalB.String(),
			"test-broker-C":  externalC.String(),
		},
	)
}

// Tests that stopping and restarting a node with a new address in a cluster will
// converge to the new address across all peers.
func TestRedeployConvergence(t *testing.T) {
	// Create 3 brokers and cluster instances to simulate 3 machines
	datadirA, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirA)
	datadirB, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirB)
	datadirC, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirC)

	brokerA, _ := broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A")})
	defer brokerA.Close()
	brokerB, _ := broker.New(&broker.Config{Name: "test-broker-B", Datadir: datadirB, Logger: log.New("tester", "B")})
	defer brokerB.Close()
	brokerC, _ := broker.New(&broker.Config{Name: "test-broker-C", Datadir: datadirC, Logger: log.New("tester", "C")})
	defer brokerC.Close()

	clusterA, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A")}, brokerA)
	defer clusterA.Close()
	clusterB, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerB.Port()}, Logger: log.New("tester", "B")}, brokerB)
	defer clusterB.Close()
	clusterC, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerC.Port()}, Logger: log.New("tester", "C")}, brokerC)
	defer clusterC.Close()

	// Join together the clusters and check that the operations are accepted
	externalA, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	externalB, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerB.Port()))
	externalC, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerC.Port()))

	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A->B: %v", err)
	}
	if err := clusterB.Join(externalC); err != nil {
		t.Fatalf("Failed to join cluster B->C: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
			"test-broker-C": externalC.String(),
		},
	)
	// Terminate one of the clusters and restart with a new address
	clusterA.Close()
	brokerA.Close()

	brokerA, _ = broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A2")})
	defer brokerA.Close()

	clusterA, _ = New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A2")}, brokerA)
	defer clusterA.Close()

	// Since there's no event happening to detect cluster downtime, force a join
	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A'->B: %v", err)
	}
	// Wait a bit for the cluster to converge and check its status (would be nice
	// to do this without waiting)
	time.Sleep(100 * time.Millisecond)

	externalA, _ = net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
			"test-broker-C": externalC.String(),
		},
	)
}

// Tests that stopping and restarting a node with a new address in a cluster will
// converge to the new address across all peers, even if the node was previously
// deemed dead.
func TestOfflineRedeployConvergence(t *testing.T) {
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	// Create 3 brokers and cluster instances to simulate 3 machines
	datadirA, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirA)
	datadirB, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirB)
	datadirC, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(datadirC)

	brokerA, _ := broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A")})
	defer brokerA.Close()
	brokerB, _ := broker.New(&broker.Config{Name: "test-broker-B", Datadir: datadirB, Logger: log.New("tester", "B")})
	defer brokerB.Close()
	brokerC, _ := broker.New(&broker.Config{Name: "test-broker-C", Datadir: datadirC, Logger: log.New("tester", "C")})
	defer brokerC.Close()

	clusterA, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A")}, brokerA)
	defer clusterA.Close()
	clusterB, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerB.Port()}, Logger: log.New("tester", "B")}, brokerB)
	defer clusterB.Close()
	clusterC, _ := New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerC.Port()}, Logger: log.New("tester", "C")}, brokerC)
	defer clusterC.Close()

	// Join together two clusters and then split them apart
	externalA, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	externalB, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerB.Port()))
	externalC, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerC.Port()))

	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A->B: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
		},
	)
	// Terminate one of the clusters and force a health check fail
	clusterA.Close()
	brokerA.Close()

	if err := clusterB.Join(externalC); err != nil {
		t.Fatalf("Failed to join cluster B->C: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	// TODO(karalabe): check partial convergence?

	// Restart the terminated cluster with a new address
	brokerA, _ = broker.New(&broker.Config{Name: "test-broker-A", Datadir: datadirA, Logger: log.New("tester", "A2")})
	defer brokerA.Close()

	clusterA, _ = New(&Config{External: &net.TCPAddr{IP: ipv4Localhost, Port: brokerA.Port()}, Logger: log.New("tester", "A2")}, brokerA)
	defer clusterA.Close()

	// Since there's no event happening to detect cluster downtime, force a join
	if err := clusterA.Join(externalB); err != nil {
		t.Fatalf("Failed to join cluster A'->B: %v", err)
	}
	// Wait a bit for the cluster to converge and check its status (would be nice
	// to do this without waiting)
	time.Sleep(100 * time.Millisecond)

	externalA, _ = net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", brokerA.Port()))
	checkClusterConvergence(t,
		[]*Cluster{clusterA, clusterB, clusterC},
		map[string]string{
			"test-broker-A": externalA.String(),
			"test-broker-B": externalB.String(),
			"test-broker-C": externalC.String(),
		},
	)
}

// checkClusterConvergence checks whether the specified clusters are converged
// on the requested broker set.
func checkClusterConvergence(t *testing.T, clusters []*Cluster, brokers map[string]string) {
	t.Helper()

	for _, cluster := range clusters {
		for name, addr := range brokers {
			if have := cluster.nodes[name].String(); have != addr {
				t.Errorf("cluster %s, broker %s: address mismatch: have %s, want %s", cluster.broker.Name(), name, have, addr)
			}
		}
		for name, addr := range cluster.nodes {
			if have := addr.String(); have != brokers[name] {
				t.Errorf("cluster %s, broker %s: unexpected address: %s", cluster.broker.Name(), name, have)
			}
		}
		for src, view := range cluster.views {
			for dst, infos := range view {
				if !infos.Alive {
					t.Errorf("cluster %s, %s->%s reported down", cluster.broker.Name(), src, dst)
				}
			}
		}
	}
}
