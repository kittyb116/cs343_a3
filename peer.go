package main

import (
	"bufio"
	"fmt"
	"log"
	//"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	//"time"
)

type BullyNode struct {
	selfID          int
	myPort          string
	nextNode		ServerConnection
}

// ServerConnection represents a connection to another node in the Raft cluster.
type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

//SERVER FUNCTION (EXPORT)
func (node *BullyNode) Reply(receivedMessage *string, reply *string) error {
	incoming_message := *receivedMessage;
	*reply = fmt.Sprintf("%s back!", incoming_message) //reply with echo of whatever it received
	return nil
}

//CLIENT FUNCTION (USE IN MAIN)
func (node *BullyNode) Message(){
	message := "hello!"
	reply := "nil"	//create a variable to store the reply

	fmt.Printf("\nmessaging the next node: %s", message)
	go func(server ServerConnection) {
		err := server.rpcConnection.Call("BullyNode.Reply", &message, &reply)
		if err != nil {
			return
		}
	}(node.nextNode)
	fmt.Printf("\nreceived reply: %s", reply)
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
		selfID:        myID,
		nextNode:      ServerConnection{},
	}

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			node.myPort = text	//save port in cluster.txt to "myPort" of current node
			index++
			//continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// --- Register the RPCs of this object of type RaftNode
	/*err = rpc.Register(node)
	if err != nil {
		log.Fatal("Error registering the RPCs", err)
	}*/
	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// fmt.Println("index stopped at ", index)

	scanner1 := bufio.NewScanner(os.Stdin)
	fmt.Printf("Type y to connect to the next node.")
	scanner1.Scan()
	input := scanner1.Text()

	if (input == "y" ){
		// Connect to next node
		var strNextNode = lines[(myID+1)%(index-1)]
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", strNextNode)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", strNextNode)
		}
		// Once connection is established, save connection information in nextNode
		node.nextNode = ServerConnection{(myID + 1) % (index - 1), strNextNode, client}
		// Record that in log
		fmt.Println("Connected to " + strNextNode)

		//CALLING THE MESSAGE FUNCTION (RPC)
		//"messaging" the next node (since we already connected to the next server)

		var wg sync.WaitGroup
		wg.Add(1)
		go node.Message() //message the next node?
		wg.Wait()  
	}
}
