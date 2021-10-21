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

	// Create the commands to run relay instances
	cmdBootnode := &cobra.Command{
		Use:   "bootnode",
		Short: "Run a multiplexer relay to upkeep the cluster",
		Run:   runRelay,
	}
	cmdBootnode.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdBootnode.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdBootnode.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdBootnode.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdBootnode.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdBootnode.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdBootnode.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdBootnode.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdBootnode.MarkFlagRequired("node.identity")
	cmdBootnode.MarkFlagRequired("node.secret")

	cmdConsensus := &cobra.Command{
		Use:   "consensus",
		Short: "Run a multiplexer relay for a consensus client",
		Run:   runRelay,
	}
	cmdConsensus.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdConsensus.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdConsensus.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdConsensus.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdConsensus.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdConsensus.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdConsensus.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdConsensus.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdConsensus.MarkFlagRequired("node.identity")
	cmdConsensus.MarkFlagRequired("node.secret")

	cmdExecution := &cobra.Command{
		Use:   "execution",
		Short: "Run a multiplexer for relay an execution client",
		Run:   runRelay,
	}
	cmdExecution.Flags().StringVar(&identityFlag, "node.identity", "", "Unique identifier for this node across the entire cluster")
	cmdExecution.Flags().StringVar(&datadirFlag, "node.datadir", filepath.Join(os.Getenv("HOME"), ".minority", "<uid>"), "Folder to persist state through restarts")
	cmdExecution.Flags().StringVar(&secretFlag, "node.secret", "", "Shared secret to authenticate and encrypt with")
	cmdExecution.Flags().StringVar(&bootnodeFlag, "node.boot", "", "Entrypoint into an existing multiplexer cluster")
	cmdExecution.Flags().StringVar(&bindAddrFlag, "bind.addr", "0.0.0.0", "Listener interface for remote multiplexers")
	cmdExecution.Flags().IntVar(&bindPortFlag, "bind.port", 4150, "Listener port for remote multiplexers")
	cmdExecution.Flags().StringVar(&extAddrFlag, "ext.addr", externalAddress(), "Advertised address for remote multiplexers")
	cmdExecution.Flags().IntVar(&extPortFlag, "ext.port", 0, "Advertised port for remote multiplexers (default = bind.port)")
	cmdExecution.MarkFlagRequired("node.identity")
	cmdExecution.MarkFlagRequired("node.secret")

	cmdRelay := &cobra.Command{
		Use:   "relay",
		Short: "Start a multiplexer relaying Ethereum APIs",
	}
	cmdRelay.AddCommand(cmdBootnode, cmdConsensus, cmdExecution)

	// Create the commands to merge relay instances
	cmdMerge := &cobra.Command{
		Use:   "merge",
		Short: "Request merging two relay clusters",
		Run:   runMerge,
	}

	rootCmd := &cobra.Command{Use: "minority"}
	rootCmd.AddCommand(cmdRelay, cmdMerge)
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

func runMerge(cmd *cobra.Command, args []string) {
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

	panic("todo")
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
