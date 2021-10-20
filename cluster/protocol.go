package cluster

// topologyTopic is the NSQ topic where topology broadcasts are published.
const topologyTopic = ".topology#ephemeral"

// update is a message sent to all other relays in the cluster to share
// the local views and allow everyone to construct an accurate global view.
type update struct {
	Owner string           `json:"owner"` // Unique ID of the node that broadcast this update
	Time  uint64           `json:"time"`  // Timestamp in ns of this update (used as seq number)
	Nodes map[string]*node `json:"nodes"` // Node ID to NSQD address mapping
}

// node
type node struct {
	Address string `json:"address"` // NSQD address to reach the broker through
	Alive   bool   `json:"alive"`   // Whether the address is reachable or not
}
