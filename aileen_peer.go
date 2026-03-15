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
	serverConnections map[int]ServerConnection //key = ID
	electionTimeout *time.Timer
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
	*reply = fmt.Sprintf("node %d has accepted leader %d", node.selfID, *receivedID)
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
	for currID := node.selfID+1; currID < node.totalNodes; currID++ { //iterate through all higher-ID peers
		conn := node.serverConnections[currID] //look up ServerConnection of ID
		//TODO: WRAP IN A GO FUNCTION (THREAD)
		var response bool
		err := conn.rpcConnection.Call("BullyNode.ReceiveLowerID", &node.selfID, &response)
		if err != nil {
			log.Println("call error:", err)
			return
		}
		if response == true{ //if ANY responds true, give up
			fmt.Printf("\nReceived reply from node %d: %t\n", currID, response)
			node.higherNodeResponded = true
			break //give up
		}
	}
	wg.Wait()
}

//LOCAL CLIENT FUNCTION (CALLS RECEIVENEWLEADER FROM ALL NODES)
func (node *BullyNode) ElectSelf() { //wg *sync.WaitGroup
	node.leaderID = node.selfID
	for ID, conn := range node.serverConnections{ //conn = ServerConnection for this ID (in serverConnections map)
		var reply string
		fmt.Printf("\nNotifying peer %d of new leader\n", ID)
		err := conn.rpcConnection.Call("BullyNode.ReceiveNewLeader", &node.selfID, &reply)
		if err != nil {
			log.Println("error receiving election:", err)
			return
		}
		fmt.Printf("\nReceived reply: %s", reply)
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
		higherNodeResponded: false,
		nominatedSelf: false,
	}

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file) 
	node.totalNodes = 0
	lines := []string{}
	for scanner.Scan() {
		text := scanner.Text() // Get server IP:port
		log.Printf("text: %s, totalNodes: %d", text, node.totalNodes) 
		if node.totalNodes == myID {
			node.myPort = text
		}
		lines = append(lines, text)
		node.totalNodes++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Register the RPCs of this object of type BullyNode
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

	//Connect to all other nodes and save Routes
	node.serverConnections = make(map[int]ServerConnection)

	for idx, address := range lines{
		if idx == node.selfID{
			continue //skip self
		}
		client, err := rpc.DialHTTP("tcp", address) // Attempt to connect to the other server node
		for err != nil { 		// If connection is not established
			log.Println("Trying again. Error: ", err)
			time.Sleep(1 * time.Second)
			client, err = rpc.DialHTTP("tcp", address)
		}
		node.serverConnections[idx] = ServerConnection{idx, address, client} //ID = index in lines = position in cluster.txt
		fmt.Printf("\nConnected to peer %d at %s\n", idx, address)
	}

	//Ticker to track nominatedSelf
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C{
			if node.nominatedSelf == true{
				fmt.Sprintf("\node %d: nominatedSelf = true!\n", node.selfID)
				node.nominatedSelf = false
				node.higherNodeResponded = false //reset everything
				node.BeginElection(&wg)
				if node.higherNodeResponded == false {
					node.ElectSelf()
				}
			}
		}
	}()

	//trigger election on input
	scanner1 := bufio.NewScanner(os.Stdin)
	fmt.Printf("Type y to begin leader election.")
	scanner1.Scan()
	input := scanner1.Text()

	if input == "y" {
		node.nominatedSelf = false
		node.higherNodeResponded = false //reset all the statuses
		node.BeginElection(&wg)
		if node.higherNodeResponded == false {
			node.ElectSelf()
		}
	}

	select{}
}	
