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
	mu sync.Mutex
	//electionTimeout *time.Timer
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
	fmt.Printf("\n[%s] Received leader: node %d\n", time.Now().Format("15:04:05.000000"), *receivedID) //TODO: nanoseconds
	*reply = fmt.Sprintf("\n[%s] Node %d has accepted leader %d\n", time.Now().Format("15:04:05.000000"), node.selfID, *receivedID)
	return nil
}

//REPLY WITH HIGHERNODERESPONDED VALUE (TRUE OR FALSE)
func (node* BullyNode) ReceiveLowerID(receivedID *int, reply *bool) error {
	fmt.Printf("\n[%s] Received lower ID: %d\n", time.Now().Format("15:04:05.000000"), *receivedID)
	if node.selfID > *receivedID{
		*reply = true
		node.mu.Lock()
		node.nominatedSelf = true
		node.mu.Unlock()
	}
	return nil
}

//LOCAL CLIENT FUNCTION (CALLS THE SERVER FUNCTIONS FROM OTHER NODES)
func (node *BullyNode) BeginElection() { //wg *sync.WaitGroup
	var wg sync.WaitGroup
	fmt.Println("Started new BeginElection")
	for currID := node.selfID+1; currID < node.totalNodes; currID++ { //iterate through all higher-ID peers
		fmt.Printf("\nCurrID: %d\n", currID)
		conn := node.serverConnections[currID] //look up ServerConnection of ID
		//fmt.Printf("\n[%s] Connected to node %d!\n", time.Now().Format("15:04:05.000000"), currID)
		wg.Add(1)
		//fmt.Printf("\n[%s] Starting new thread for %d\n", time.Now().Format("15:04:05.000000"), currID)
		go func(id int){ //create go function (thread) for each node message
			defer wg.Done()
			var response bool
			err := conn.rpcConnection.Call("BullyNode.ReceiveLowerID", &node.selfID, &response)
			if err != nil {
				log.Println("Call error:", err)
				return
			}
			if response == true{ //if ANY responds true, give up
				fmt.Printf("\n[%s] Received reply from node %d: %t\n", time.Now().Format("15:04:05.000000"), id, response)
				node.mu.Lock()
				node.higherNodeResponded = true
				node.mu.Unlock()
				return //give up
			}
		}(currID)
		//fmt.Printf("\n[%s] Finished thread for %d\n", time.Now().Format("15:04:05.000000"), currID)
	}
	wg.Wait() //end function when all messages are done
}

//LOCAL CLIENT FUNCTION (CALLS RECEIVENEWLEADER FROM ALL NODES)
func (node *BullyNode) ElectSelf() { //wg *sync.WaitGroup
	node.leaderID = node.selfID
	var wg sync.WaitGroup
	fmt.Printf("\n[%s] Started new electSelf\n", time.Now().Format("15:04:05.000000"))
	for ID, conn := range node.serverConnections{ //conn = ServerConnection for this ID (in serverConnections map)
		fmt.Printf("\n[%s] Notifying peer %d of new leader\n", time.Now().Format("15:04:05.000000"), ID)
		wg.Add(1)
		go func(c ServerConnection){ //notify each node in a new goroutine
			var reply string
			defer wg.Done()
			err := c.rpcConnection.Call("BullyNode.ReceiveNewLeader", &node.selfID, &reply)
			if err != nil {
				log.Println("Error receiving election:", err)
				return
			}
			fmt.Printf("\n[%s] Received reply: %s", time.Now().Format("15:04:05.000000"), reply)
		}(conn)
	}
	wg.Wait()
	fmt.Printf("\n[%s] Completed this electSelf\n", time.Now().Format("15:04:05.000000"))
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
		if node.totalNodes == myID { //if "nodes so far" = myID, then current ID = myID
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

	//var wg sync.WaitGroup //create separate threads for server + client

	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
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
				fmt.Printf("\n[%s] Node %d: nominatedSelf = true!\n", time.Now().Format("15:04:05.000000"), node.selfID)
				node.mu.Lock() //lock nominatedSelf and higherNodeResponded while editing them
				node.nominatedSelf = false
				node.higherNodeResponded = false //reset everything
				node.mu.Unlock()
				node.BeginElection()
				if node.higherNodeResponded == false {
					node.ElectSelf()
				}
			}
		}
	}()

	//trigger election on input
	scanner1 := bufio.NewScanner(os.Stdin)
	fmt.Println("Type y to begin leader election.")
	scanner1.Scan()
	input := scanner1.Text()

	if input == "y" {
		node.mu.Lock() //lock nominatedSelf, higherNodeResponded while editing them
		node.nominatedSelf = false
		node.higherNodeResponded = false //reset all the statuses
		node.mu.Unlock()
		node.BeginElection()
		if node.higherNodeResponded == false {
			node.ElectSelf()
		}
	}

	select{}
}	
