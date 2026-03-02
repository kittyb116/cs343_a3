package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// RingNode represents a single node in the cluster, providing the
// necessary methods to handle RPCs.
type RingNode struct {
	mutex           sync.Mutex
	selfID          int
	leaderID        int
	myPort          string
	nextNode        ServerConnection
	nominatedSelf   bool
	electionTimeout *time.Timer
}

// RingVote contains the candidate's information that is needed for the
// node receiving the RingVote to decide whether to vote for the candidate
// in question.
type RingVote struct {
	CandidateID int
	IsTerminal  bool
}

// -----------------------------------------------------------------------------
// Leader Election
// -----------------------------------------------------------------------------

// RequestVote handles incoming RequestVote RPCs from other servers.
// It checks its own logs to confirm eligibility for leadership
// if the candidateID received is higher that server's own ID, then vote is accepted and passed
// if less than server's own ID, the vote is ignored
// if candidateID is the same as serverID, then election won, and server can confirm its leadership
func (node *RingNode) RequestVote(receivedVote RingVote, acknowledge *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Println("Vote message received for ", receivedVote.CandidateID)

	ackReply := "nil"

	// Leader has been identified
	if receivedVote.IsTerminal {
		if node.leaderID != receivedVote.CandidateID {
			node.leaderID = receivedVote.CandidateID
			fmt.Println("Leader has been elected: ", receivedVote.CandidateID)
			// Pass result of the election to nextNode
			theNextVote := RingVote{
				CandidateID: receivedVote.CandidateID,
				IsTerminal:  receivedVote.IsTerminal,
			}
			go func(server ServerConnection) {
				err := server.rpcConnection.Call("RingNode.RequestVote", theNextVote, &ackReply)
				if err != nil {
					return
				}
			}(node.nextNode)
			// Synchronous version
			// node.nextNode.rpcConnection.Call("RingNode.RequestVote", theNextVote, -1)
		}
		return nil
	}
	// Reject vote request if received candidateID is smaller
	if receivedVote.CandidateID < node.selfID {
		fmt.Println("Received a vote from lower Id of ", receivedVote.CandidateID)
		// If node have not already nominated itself as leader,
		// then pass nomiation of selfID to nextNode
		if !node.nominatedSelf {
			theNextVote := RingVote{
				CandidateID: node.selfID,
				IsTerminal:  false,
			}
			go func(server ServerConnection) {
				err := server.rpcConnection.Call("RingNode.RequestVote", theNextVote, &ackReply)
				if err != nil {
					return
				}
			}(node.nextNode)
			// Synchronous version
			// node.nextNode.rpcConnection.Call("RingNode.RequestVote", theNextVote, -1)
		}
		return nil
	}
	if receivedVote.CandidateID > node.selfID {
		fmt.Println("Received a vote from higher Id of ", receivedVote.CandidateID)
		// Pass nomiation of candidateID to nextNode
		theNextVote := RingVote{
			CandidateID: receivedVote.CandidateID,
			IsTerminal:  false,
		}
		go func(server ServerConnection) {
			err := server.rpcConnection.Call("RingNode.RequestVote", theNextVote, &ackReply)
			if err != nil {
				return
			}
		}(node.nextNode)
		// Synchronous version
		// node.nextNode.rpcConnection.Call("RingNode.RequestVote", theNextVote, -1)
		return nil
	}

	// Final case is when node receives its own nomination
	fmt.Println("Received self vote again. Yay, node is leader!")
	// Pass nomiation of candidateID to nextNode
	theNextVote := RingVote{
		CandidateID: receivedVote.CandidateID,
		IsTerminal:  true,
	}
	go func(server ServerConnection) {
		err := server.rpcConnection.Call("RingNode.RequestVote", theNextVote, &ackReply)
		if err != nil {
			return
		}
	}(node.nextNode)
	// Synchronous version
	// node.nextNode.rpcConnection.Call("RingNode.RequestVote", theNextVote, -1)

	return nil
}

func (node *RingNode) LeaderElection() {
	// This is an option to limit who can start the leader election
	// Recommended if you want only a specific process to start the election
	// if node.selfID != 2 {
	// 	return
	// }

	for {
		// Wait for election timeout
		// Uncomment this if you decide to do periodic leader election

		// <-node.electionTimeout.C

		// Check if node is already leader so loop does not continue
		if node.leaderID == node.selfID {
			fmt.Println("Ending leader election because I am now leader")
			return
		}

		// Initialize election by incrementing term and voting for self
		arguments := RingVote{
			CandidateID: node.selfID,
			IsTerminal:  false,
		}
		ackReply := "nil"

		// Sending nomination message
		fmt.Println("Requesting votes from ", node.nextNode.serverID, node.nextNode.Address)
		go func(server ServerConnection) {
			err := server.rpcConnection.Call("RingNode.RequestVote", arguments, &ackReply)
			if err != nil {
				return
			}
		}(node.nextNode)

		// If you want leader election to be restarted periodically,
		// Uncomment the next line
		// I do not recommend when debugging

		// node.resetElectionTimeout()
	}
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (node *RingNode) resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
}

func main(){
	//MAIN FUNCTION FROM RING IMPLEMENTATION
	// Start the election using a timer
	// Uncomment the next 3 lines, if you want leader election to be initiated periodically
	// I do not recommend it during debugging

	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	// node.electionTimeout = time.NewTimer(tRandom)

	var wg sync.WaitGroup
	wg.Add(1)
	go node.LeaderElection() // Concurrent leader election, which can be made non-stop with timers
	wg.Wait()                // Waits forever, so main process does not stop
}