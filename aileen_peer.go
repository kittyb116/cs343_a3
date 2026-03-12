package main

import (
	"bufio"
	"fmt"
	"log"
	"sync"
	//"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type BullyNode struct {
	selfID   int
	leaderID int
	myPort   string
	higherNodeResponded   bool
	totalNodes int
	nominatedSelf bool //determine whether node will start an election
	electionTimeout *time.Timer
	lines []string
	nextNode ServerConnection
}

// ServerConnection represents a connection to another node in the cluster.
type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

//SERVER FUNCTION (RECEIEVE NEW LEADER)
func (node *BullyNode) ReceiveNewLeader(receivedID *int, reply *string) error {
	node.leaderID = *receivedID
	*reply = fmt.Sprintf(" 'node %d has accepted leader %d' ", node.selfID, *receivedID)
	return nil
}

//REPLY WITH HIGHERNODERESPONDED VALUE (TRUE OR FALSE)
func (node* BullyNode) ReceiveLowerID(receivedID int, reply *bool) error {
	if node.selfID > receivedID{
		*reply = true
		node.nominatedSelf = true
	}
	return nil
}

//LOCAL CLIENT FUNCTION (CALLS THE SERVER FUNCTIONS FROM OTHER NODES)
func (node *BullyNode) BeginElection(wg *sync.WaitGroup) {
	//TOOD: Make a waitgroup
	//iterate through all higher-ID peers
	for i := node.selfID+1; i < node.totalNodes; i++ { //index = total number of ports = len(lines)
		var strNextNode = node.lines[i]

		//CONNECTION ATTEMPT START
		client, err := rpc.DialHTTP("tcp", strNextNode) // Attempt to connect to the other server node
		for err != nil { 		// If connection is not established
			log.Println("Trying again. Error: ", err)
			time.Sleep(1 * time.Second)
			client, err = rpc.DialHTTP("tcp", strNextNode)
		}
		// Once connection is established, save connection information in nextNode
		node.nextNode = ServerConnection{i, strNextNode, client} //i is next node ID
		fmt.Printf("\nConnected to peer %d at %s\n", i, strNextNode)
		//CONNECTION ATTEMPT END

		//TODO: WRAP IN A GO FUNCTION (THREAD)
		var response bool
		err = node.nextNode.rpcConnection.Call("BullyNode.ReceiveLowerID", &node.selfID, &response)
		if err != nil {
			log.Println("call error:", err)
			return
		}
		if response == true{ //if ANY responds true, give up
			fmt.Printf("\nreceived reply from node %d: %t\n", i, response)
			node.higherNodeResponded = true
			break //give up
		}
	}
	wg.Wait()
}

//LOCAL CLIENT FUNCTION (CALLS RECEIVENEWLEADER FROM ALL NODES)
func (node *BullyNode) ElectSelf() { //wg *sync.WaitGroup
	node.leaderID = node.selfID
	for i := 0; i < node.totalNodes; i++ {
		if i == node.selfID{
			continue
		}
		var strNextNode = node.lines[i]
		//wg.Add(1)
		client, err := rpc.DialHTTP("tcp", strNextNode) // Attempt to connect to the other server node
		for err != nil { 		// If connection is not established
			log.Println("Trying again. Error: ", err)
			time.Sleep(1 * time.Second)
			client, err = rpc.DialHTTP("tcp", strNextNode)
		}
		node.nextNode = ServerConnection{i, strNextNode, client} //i is next node ID
		fmt.Printf("\nConnected to peer %d at %s\n", i, strNextNode)
		var reply string
		fmt.Printf("\nNotifying peer %d of leader status\n", i)
		err = node.nextNode.rpcConnection.Call("BullyNode.ReceiveNewLeader", &node.selfID, &reply)
		if err != nil {
			log.Println("error receiving election:", err)
			return
		}
	}
}

// -----------------------------------------------------------------------------
func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// --- Read the values sent in the command line
	// Get this server's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	node := &BullyNode{
		selfID:   myID,
		nextNode: ServerConnection{},
		higherNodeResponded: false,
		nominatedSelf: false,
	}

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	//lines := make([]string, 0)
	node.totalNodes = 0
	//index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, node.totalNodes)
		if node.totalNodes == myID {
			node.myPort = text //save port in cluster.txt to "myPort" of current node
			//index++
			//continue
		}
		node.lines = append(node.lines, text) //modified lines: append all ports
		node.totalNodes++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// --- Register the RPCs of this object of type BullyNode
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("Error registering the RPCs", err)
	}

	var wg sync.WaitGroup //create separate threads for server + client

	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	//defer wg.Done()
	log.Printf("\nServing rpc on port " + node.myPort)

	time.Sleep(5 * time.Second) //option to make sleep timer so nodes connect after a second

	if node.nominatedSelf == true{
		node.BeginElection(&wg)
	}
	
	//trigger election on input
	scanner1 := bufio.NewScanner(os.Stdin)
	fmt.Printf("Type y to begin leader election.")
	scanner1.Scan()
	input := scanner1.Text()

	if input == "y" {
		node.BeginElection(&wg)
		if node.higherNodeResponded == false {
			node.ElectSelf()
		} else{
			return
		}
	}
}	
