// Package cluster tracks the broker cluster and communicates thorough it.
package cluster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/karalabe/minority/broker"
	"github.com/nsqio/go-nsq"
	"github.com/olekukonko/tablewriter"
)

// Config is the set of options to fine tune the messaging cluster.
type Config struct {
	ExternalIP   *net.IPAddr // External IP address to advertise for the local broker
	ExternalPort int         // External port number to advertise for the local broker

	Logger log.Logger // Logger to allow differentiating clusters if many is embedded
}

// API request to have the local cluster join with a remote one.
type joinRequest struct {
	address *net.TCPAddr // Remote address to join with
	result  chan error   // Result of the join operation
}

type Cluster struct {
	address *net.TCPAddr   // External address of the local broker
	broker  *broker.Broker // Broker through which to communicate

	nodes map[string]*net.TCPAddr     // Known remote relays and their addresses
	times map[string]uint64           // Timestamps of the last broker updates
	views map[string]map[string]*node // Remote views of the broker cluster
	prods map[string]*nsq.Producer    // Producers writing into each remote broker

	join chan *joinRequest // Channel for requesting joining a remote cluster

	logger log.Logger      // Logger to allow differentiating clusters if many is embedded
	quit   chan chan error // Termination channel to tear down the node
	term   chan struct{}   // Notification channel of termination
}

// New initializes a new cluster and starts making and maintaining connections
// to remote nodes.
func New(config *Config, broker *broker.Broker) (*Cluster, error) {
	// Create an idle cluster
	self := broker.Name()

	address := &net.TCPAddr{
		IP:   config.ExternalIP.IP,
		Port: config.ExternalPort,
		Zone: config.ExternalIP.Zone,
	}
	logger := config.Logger
	if logger == nil {
		logger = log.New()
	}
	cluster := &Cluster{
		address: address,
		broker:  broker,
		nodes: map[string]*net.TCPAddr{
			self: address,
		},
		times: map[string]uint64{
			self: uint64(time.Now().UnixNano()),
		},
		views: map[string]map[string]*node{
			self: make(map[string]*node),
		},
		prods:  make(map[string]*nsq.Producer),
		join:   make(chan *joinRequest),
		logger: logger,
		quit:   make(chan chan error),
		term:   make(chan struct{}),
	}
	go cluster.maintain()
	return cluster, nil
}

// Close terminates the cluster maintenance routines, disconnects all the message
// producers and consumers and returns any previous faults.
func (c *Cluster) Close() error {
	// Request the maintenance routine to b torn down
	errc := make(chan error)
	select {
	case c.quit <- errc:
	case <-c.term:
		return nil // already terminated
	}
	err := <-errc

	// Terminate any producers still connected
	for _, producer := range c.prods {
		producer.Stop()
	}
	c.prods = nil

	// Return any failures hit in the maintenance
	close(c.term)
	return err
}

// Join requests the local peer to join a remote cluster via a remote rendezvous
// peer. The local and remote cluster will automatically merge if the connection
// succeeds.
func (c *Cluster) Join(peer *net.TCPAddr) error {
	res := make(chan error)

	select {
	case c.join <- &joinRequest{address: peer, result: res}:
		return <-res
	case <-c.term:
		return errors.New("cluster terminating")
	}
}

// maintain is a background process that subscribes to all remote brokers and
// keeps exchanging cluster topology information with them, trying to keep the
// entire thing in one piece.
func (c *Cluster) maintain() {
	// Create a new topology consumer and stream updates into a maintenance channel
	consumer, err := c.broker.NewConsumer(topologyTopic)
	if err != nil {
		panic(err)
	}
	defer consumer.Stop()

	updateCh := make(chan *update)
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		update := new(update)
		if err := json.Unmarshal(msg.Body, update); err != nil {
			return err
		}
		updateCh <- update
		return nil
	}))
	// Exchange topology messages until a failure or the cluster is torn down
	var errc chan error
	for errc == nil && err == nil {
		// Iterate over all the known brokers and connect the consumer to any
		// that's not yet connected. Since there's no easy way to retrieve which
		// is and which isn't, just YOLO it and handle duplicates.
		for _, addr := range c.nodes {
			switch err := consumer.ConnectToNSQD(addr.String()); err {
			case nsq.ErrAlreadyConnected:
				// We're still alive, nothing to do
			case nil:
				// New broker connected, waiting for events
			default:
				// Connection failed
			}
		}
		// Iterate over all the known brokers and connect new producers to any
		// that's not yet connected.
		var changed bool

		for name, addr := range c.nodes {
			// If a producer exists for the specific remote broker, try to ping
			// it as a health check; removing any failed producers (as well as
			// the consumer, since we assume they are symmetric).
			if producer, ok := c.prods[name]; ok {
				if err := producer.Ping(); err != nil {
					c.logger.Warn("Remote broker became unreachable", "name", name, "err", err)
					producer.Stop()
					consumer.DisconnectFromNSQD(addr.String())

					delete(c.prods, name)
					c.views[c.broker.Name()][name].Alive = false

					changed = true // update the node's view of the cluster
				}
			}
			// If a producer does not exist (or was just disconnected), try to
			// reconnect with it either way.
			if _, ok := c.prods[name]; !ok {
				producer, err := c.broker.NewProducer(addr.String())
				if err != nil {
					continue
				}
				if producer.Ping() != nil {
					producer.Stop()
					continue
				}
				c.prods[name] = producer

				self := c.broker.Name()
				c.views[self][name] = &node{
					Address: addr.String(),
					Alive:   true,
				}
				changed = true // update the node's view of the cluster
			}
		}
		// If the local view of the network was changed, generate an update message
		// and publish it with all live producers
		if changed {
			msg := &update{
				Owner: c.broker.Name(),
				Time:  uint64(time.Now().UnixNano()),
				Nodes: c.views[c.broker.Name()],
			}
			blob, err := json.Marshal(msg)
			if err != nil {
				panic(err) // Can't fail, panic during development
			}
			for _, producer := range c.prods {
				if err := producer.Publish(topologyTopic, blob); err != nil {
					c.logger.Warn("Failed to publish topology update", "err", err)
				}
			}
			// Mark the local view updated to keep the timestamps correlated
			c.times[msg.Owner] = msg.Time

			// Finally, report the stats to the user for debugging
			c.reportStats()
		}
		select {
		case errc = <-c.quit:
			continue

		case req := <-c.join:
			// User request received to join a remote cluster, make sure it's
			// not yet connected
			logger := c.logger.New("addr", req.address)
			logger.Info("Requesting to join remote cluster")

			var known string
			for name, addr := range c.nodes {
				if req.address.String() == addr.String() {
					known = name
					break
				}
			}
			if known != "" {
				logger.Info("Requested peer already known", "name", known)
				req.result <- fmt.Errorf("peer already known as %s", known)
				continue
			}
			// Peer not yet known, create a temporary producer to check that the
			// remote peer accepts inbound connections and uses the same secret.
			producer, err := c.broker.NewProducer(req.address.String())
			if err != nil {
				logger.Warn("Failed to connect to remote peer", "err", err)
				req.result <- err
				continue
			}
			if err := producer.Ping(); err != nil {
				logger.Warn("Failed to check remote health", "err", err)
				producer.Stop()
				req.result <- err
				continue
			}
			// Remote peer alive and mutual authentication passed. Push a topology
			// update and drop the producer. This will force the members of the
			// remote cluster to dial back and give us their true external address
			// and name even if the user supplied something weird.
			msg := &update{
				Owner: c.broker.Name(),
				Time:  c.times[c.broker.Name()],
				Nodes: c.views[c.broker.Name()],
			}
			blob, err := json.Marshal(msg)
			if err != nil {
				panic(err) // Can't fail, panic during development
			}
			if err := producer.Publish(topologyTopic, blob); err != nil {
				c.logger.Warn("Failed to publish topology update", "err", err)
			}
			producer.Stop()

			req.result <- nil

		case update := <-updateCh:
			// Everyone receives their own updates too. Ignore those as they are
			// useless (also prevents a duplicate name from overwriting the local
			// view).
			logger := c.logger.New("updater", update.Owner, "seqnum", update.Time)
			if update.Owner == c.broker.Name() {
				logger.Debug("Ignoring self update")
				continue
			}
			// Topology update received, if it's stale or duplicate, ignore. As
			// we're publishing and consuming on a full graph, each update will
			// be received the number of nodes times.
			if c.times[update.Owner] >= update.Time {
				logger.Debug("Ignoring stale update")
				continue
			}
			// Topology update newer, integrate it into the local view
			logger.Info("Updating topology with remote view")

			c.times[update.Owner] = update.Time
			c.views[update.Owner] = update.Nodes

			for name, infos := range update.Nodes {
				// Make sure the address is valid and drop any invalid entries
				addr, err := net.ResolveTCPAddr("tcp", infos.Address)
				if err != nil {
					logger.Warn("Failed to resolve advertised address", "name", name, "addr", infos.Address, "err", err)
					delete(c.views[update.Owner], name)
					continue
				}
				// If the remote broker is unknown, start tracking it
				if _, ok := c.nodes[name]; !ok {
					// Address clashes might happen when broker names are changed
					var (
						renamed  string
						accepted bool
					)
					for oldName, oldAddr := range c.nodes {
						if oldAddr.String() == infos.Address {
							// Mark the node renamed - whether we accept it or not -
							// to avoid discovering it as new too
							renamed = oldName

							// If a node advertises the same name as an old one,
							// check any live connections and reject or accept
							// based on that.
							if producer, ok := c.prods[renamed]; ok {
								if producer.Ping() == nil {
									// Old connection still valid, ignore advertised address
									logger.Warn("Rejecting new name for healthy broker", "addr", infos.Address, "old", renamed, "new", name)
									break
								}
								// Ping failed, perhaps the broker just renamed, update all routes
								logger.Warn("Accepted new name for failing broker", "addr", infos.Address, "old", renamed, "new", name)
								accepted = true

								consumer.DisconnectFromNSQD(oldAddr.String())
								producer.Stop()
								break
							}
							// No previous connection to the broker was maintained,
							// overwrite the name associated with the address
							logger.Warn("Accepted new name for dead broker", "addr", infos.Address, "old", renamed, "new", name)
							accepted = true
							break
						}
					}
					if renamed != "" && accepted {
						c.nodes[name] = addr

						delete(c.nodes, renamed)
						delete(c.times, renamed)
						delete(c.views, renamed)
						delete(c.prods, renamed)

						delete(c.views[c.broker.Name()], renamed)
						continue
					}
					logger.Info("Discovered new remote broker", "name", name, "addr", infos.Address)
					c.nodes[name] = addr
					continue
				}
				// If the remote broker was already known, use majority live address
				if old := c.nodes[name].String(); old != infos.Address {
					// If we have a live producer, ping the remote node to double-check
					if producer, ok := c.prods[name]; ok {
						if producer.Ping() == nil {
							// Old connection still valid, ignore advertised address
							logger.Warn("Rejecting new address for healthy broker", "name", name, "old", old, "new", infos.Address)
							continue
						}
						// Ping failed, perhaps the broker just migrated, update all routes
						logger.Warn("Accepted new address for failing broker", "name", name, "old", old, "new", infos.Address)
						c.nodes[name] = addr
						c.views[c.broker.Name()][name].Alive = false

						consumer.DisconnectFromNSQD(old)
						producer.Stop()
						delete(c.prods, name)
						continue
					}
					// No previous connection to the broker was maintained,
					// overwrite the address associated with the name
					logger.Warn("Accepted new address for dead broker", "name", name, "old", old, "new", infos.Address)
					c.nodes[name] = addr
				}
			}
			c.reportStats()
		}
	}
	// If the loop was stopped due to an error, wait for the termination request
	// and then return. Otherwise, just feed the request a nil error.
	if errc == nil {
		errc = <-c.quit
	}
	errc <- err
}

// reportStats is a debug method to print the current cluster topology and any
// other stats that might be useful.
func (c *Cluster) reportStats() {
	var (
		buffer = new(bytes.Buffer)
		stats  = bufio.NewWriter(buffer)
	)
	// Print some stats about the local broker
	fmt.Fprintf(stats, "Broker name:    %s\n", c.broker.Name())
	fmt.Fprintf(stats, "Broker address: %v\n", c.address)
	fmt.Fprintf(stats, "\n")

	// Print the members of the broker cluster
	brokers := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		brokers = append(brokers, name)
	}
	sort.Strings(brokers)

	c.reportClusterMembers(stats, brokers)
	c.reportConnectionMatrix(stats, brokers)
	c.reportUnaccountedBrokers(stats, brokers)

	// Flush the entire stats to the console
	stats.Flush()
	c.logger.Info("Updated cluster topology\n\n" + string(buffer.Bytes()))
}

// reportClusterMembers creates a membership table to report which brokers the
// local node knows about and whether other nodes agree or not.
func (c *Cluster) reportClusterMembers(w io.Writer, brokers []string) {
	fmt.Fprintf(w, "Cluster members:\n")

	members := make([][]string, 0, len(brokers))
	for i, broker := range brokers {
		var (
			id   = strconv.Itoa(i + 1)
			addr = c.nodes[broker].String()
			age  = time.Since(time.Unix(0, int64(c.times[broker]))).String()

			agree    = make([]string, 0, len(brokers))
			disagree = make([]string, 0, len(brokers))
			unknown  = make([]string, 0, len(brokers))
		)
		for j, name := range brokers {
			if c.views[name][broker] == nil {
				unknown = append(unknown, strconv.Itoa(j+1))
			} else if c.views[name][broker].Address != addr {
				disagree = append(disagree, strconv.Itoa(j+1))
			} else {
				agree = append(agree, strconv.Itoa(j+1))
			}
		}
		members = append(members, []string{id, broker, addr, age,
			fmt.Sprintf("%v", agree), fmt.Sprintf("%v", disagree), fmt.Sprintf("%v", unknown)})
	}
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"#", "Name", "Address", "Updated", "Agree", "Disagree", "Unaware"})
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.AppendBulk(members)
	table.Render()

	fmt.Fprintf(w, "\n")
}

// reportConnectionMatrix creates a connection matrix to report on which broker
// reports being connected to which other brokers.
func (c *Cluster) reportConnectionMatrix(w io.Writer, brokers []string) {
	fmt.Fprintf(w, "Connection matrix:\n")

	header := make([]string, 0, len(brokers))
	matrix := make([][]string, 0, len(brokers))

	for i, broker := range brokers {
		connected := make(map[int]bool)
		for name, node := range c.views[broker] {
			if idx := sort.SearchStrings(brokers, name); idx < len(brokers) && brokers[idx] == name {
				connected[idx] = node.Alive
			}
		}
		row := []string{strconv.Itoa(i + 1)}
		for idx := 0; idx < len(brokers); idx++ {
			if connected[idx] {
				row = append(row, "Y")
			} else {
				row = append(row, "N")
			}
		}
		header = append(header, strconv.Itoa(i+1))
		matrix = append(matrix, row)
	}
	table := tablewriter.NewWriter(w)
	table.SetHeader(append([]string{""}, header...))
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.AppendBulk(matrix)
	table.Render()

	fmt.Fprintf(w, "\n")
}

// reportUnaccountedBrokers creates a report on brokers that remove nodes have
// advertised, but for some reason the local node rejected them.
func (c *Cluster) reportUnaccountedBrokers(w io.Writer, brokers []string) {
	unaccounted := make([][]string, 0, len(brokers))
	for src, view := range c.views {
		var extras []string
		for dst, _ := range view {
			if idx := sort.SearchStrings(brokers, dst); idx == len(brokers) || brokers[idx] != dst {
				extras = append(extras, dst)
			}
		}
		if len(extras) > 0 {
			sort.Strings(extras)
			unaccounted = append(unaccounted, append([]string{src}, extras[0], view[extras[0]].Address))
			for _, extra := range extras[1:] {
				unaccounted = append(unaccounted, append([]string{""}, extra, view[extra].Address))
			}
		}
	}
	if len(unaccounted) == 0 {
		return
	}
	fmt.Fprintf(w, "Dangling views:\n")

	table := tablewriter.NewWriter(w)
	table.SetHeader(append([]string{"Source", "Target", "Address"}))
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.AppendBulk(unaccounted)
	table.Render()

	fmt.Fprintf(w, "\n")
}
