package main

import (
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/log"
	"github.com/karalabe/minority/broker"
	"github.com/karalabe/minority/cluster"
	"github.com/spf13/cobra"
)

var (
	identityFlag string
	datadirFlag  string
	secretFlag   string
	bootnodeFlag string
	bindAddrFlag string
	bindPortFlag int
	extAddrFlag  string
	extPortFlag  int
)

func main() {
	// Configure the logger to print everything
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	var cmdBootnodeRelay = &cobra.Command{
		Use:   "bootnode",
		Short: "Run a noop multiplexer to help maintain the cluster",
		Run:   runRelay,
	}
	cmdBootnodeRelay.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdBootnodeRelay.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdBootnodeRelay.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdBootnodeRelay.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdBootnodeRelay.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdBootnodeRelay.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdBootnodeRelay.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdBootnodeRelay.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdBootnodeRelay.MarkFlagRequired("node.identity")
	cmdBootnodeRelay.MarkFlagRequired("node.secret")

	var cmdConsensusRelay = &cobra.Command{
		Use:   "consensus",
		Short: "Run a multiplexer for a consensus client",
		Run:   runRelay,
	}
	cmdConsensusRelay.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdConsensusRelay.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdConsensusRelay.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdConsensusRelay.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdConsensusRelay.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdConsensusRelay.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdConsensusRelay.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdConsensusRelay.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdConsensusRelay.MarkFlagRequired("node.identity")
	cmdConsensusRelay.MarkFlagRequired("node.secret")

	var cmdExecutionRelay = &cobra.Command{
		Use:   "execution",
		Short: "Run a multiplexer for an execution client",
		Run:   runRelay,
	}
	cmdExecutionRelay.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdExecutionRelay.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdExecutionRelay.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdExecutionRelay.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdExecutionRelay.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdExecutionRelay.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdExecutionRelay.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdExecutionRelay.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdExecutionRelay.MarkFlagRequired("node.identity")
	cmdExecutionRelay.MarkFlagRequired("node.secret")

	var rootCmd = &cobra.Command{Use: "minority"}
	rootCmd.AddCommand(cmdBootnodeRelay, cmdConsensusRelay, cmdExecutionRelay)
	rootCmd.Execute()
}

func runRelay(cmd *cobra.Command, args []string) {
	// Configure and start the message broker
	brokerConfig := &broker.Config{
		Name:    identityFlag,
		Datadir: strings.Replace(datadirFlag, "<uid>", identityFlag, -1),
		Secret:  secretFlag,
		Listener: &net.TCPAddr{
			IP:   net.ParseIP(bindAddrFlag),
			Port: bindPortFlag,
		},
	}
	broker, err := broker.New(brokerConfig)
	if err != nil {
		log.Crit("Failed to start message broker", "err", err)
	}
	defer broker.Close()

	// Configure and start the cluster manager
	if extPortFlag == 0 {
		extPortFlag = bindPortFlag
	}
	clusterConfig := &cluster.Config{
		External: &net.TCPAddr{
			IP:   net.ParseIP(extAddrFlag),
			Port: extPortFlag,
		},
	}
	cluster, err := cluster.New(clusterConfig, broker)
	if err != nil {
		log.Crit("Failed to start message cluster", "err", err)
	}
	defer cluster.Close()

	// If a bootnode was specified, explicitly connect to it
	if bootnodeFlag != "" {
		bootnodeAddr, err := net.ResolveTCPAddr("tcp", bootnodeFlag)
		if err != nil {
			log.Crit("Failed to resolve bootnode address", "err", err)
		}
		if err := cluster.Join(bootnodeAddr); err != nil {
			log.Crit("Failed to join to bootnode", "err", err)
		}
	}
	// Wait until the process is terminated
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
}

// externalAddress iterates over all the network interfaces of the machine and
// returns the first non-loopback one (or the loopback if none can be found).
func externalAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Crit("Failed to retrieve network interfaces", "err", err)
	}
	for _, iface := range ifaces {
		// Skip disconnected and loopback interfaces
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			log.Warn("Failed to retrieve network addresses", "err", err)
			continue
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				return v.IP.String()
			case *net.IPAddr:
				return v.IP.String()
			}
		}
	}
	return "127.0.0.1"
}
