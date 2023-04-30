package Replica

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	path       = flag.String("path", "config", "the path to config file")
	selfConfig = flag.String("self", "self", "the path to self configuration file")
)

type Node struct {
	nodeId int
	addr   string
}

func readNodeConfig() (int, string, int, error) {
	nodeFile, err := os.Open(*selfConfig)
	if err != nil {
		log.Fatalf("Error to load node configuration file.")
		return 0, "", 0, err
	}
	defer nodeFile.Close()
	scanner := bufio.NewScanner(nodeFile)
	lineNumber := 0
	var nodeId int
	var ip string
	var port int
	for scanner.Scan() {
		line := scanner.Text()
		if lineNumber == 0 {
			nodeId, _ = strconv.Atoi(line)
		} else {
			parts := strings.Split(line, ":")
			ip = parts[0]
			port, _ = strconv.Atoi(parts[1])
		}
		lineNumber++
	}
	return nodeId, ip, port, nil
}
func readClusterConfig() ([]Node, error) {
	// The format of config file:
	// repeated nodeId-Ip:port
	config, err := os.Open(*path)
	if err != nil {
		log.Fatalf("Error to load cluster configuration file.")
		return nil, err
	}
	defer config.Close()
	scanner := bufio.NewScanner(config)
	lineNumber := 0
	var nodes []Node
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "-")
		nodeId, _ := strconv.Atoi(parts[0])
		nodes = append(nodes, Node{
			nodeId: nodeId,
			addr:   parts[1],
		})
		lineNumber++
	}
	return nodes, nil
}
func main() {
	flag.Parse()
	nodeId, ip, port, err := readNodeConfig()
	if err != nil {
		return
	}
	log.Printf("Node %d, begin to load and address is %s"+": %d", nodeId, ip, port)
	cur := &Node{
		nodeId: nodeId,
		addr:   ip + strconv.Itoa(port),
	}

	nodes, err := readClusterConfig()
	if err != nil {
		return
	}

}
