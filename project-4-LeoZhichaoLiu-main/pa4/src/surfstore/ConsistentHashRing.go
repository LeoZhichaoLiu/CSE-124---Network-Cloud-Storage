package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"sort"	
	"fmt"
	"strconv"
)

type Node struct {
	Addr  string
	Index int
}

type ConsistentHashRing struct {
	RingSize int
	Nodes    []Node
}

// Perform a modulo operation on a hash string.
// The hash string is assumed to be hexadecimally encoded.
func HashMod(hashString string, ringSize int) int {
	hashBytes, _ := hex.DecodeString(hashString)
	hashInt := new(big.Int).SetBytes(hashBytes[:])
	ringSizeInt := big.NewInt(int64(ringSize))

	indexInt := new(big.Int).Mod(hashInt, ringSizeInt)

	return int(indexInt.Int64())
}

// Compute a block’s index on the ring from its hash value.
func (ms *ConsistentHashRing) ComputeBlockIndex(blockHash string) int {
	return HashMod(blockHash, ms.RingSize)
}

// Compute a node’s index on the ring from its address string.
func (ms *ConsistentHashRing) ComputeNodeIndex(nodeAddr string) int {
	hashBytes := sha256.Sum256([]byte(nodeAddr))
	hashString := hex.EncodeToString(hashBytes[:])
	return HashMod(hashString, ms.RingSize)
}

// Find the hosting node for the given ringIndex. It’s basically the first node on the ring with node.Index >= ringIndex (in a modulo sense).
func (ms *ConsistentHashRing) FindHostingNode(ringIndex int) Node {
	// Try to implement a O(log N) solution here using binary search.
	// It's also fine if you can't because we don't test your perforrmance.
	//panic("todo")
    
	// We loop through every nodes (in order of index), return the first node with index >= ring index.
	for _, node := range (ms.Nodes) {
        if node.Index >= ringIndex {
			return node
		}
	}
	return ms.Nodes[0]
}

// Add the given nodeAddr to the ring.
func (ms *ConsistentHashRing) AddNode(nodeAddr string) {
	// O(N) solution is totally fine here.
	// O(log N) solution might be overly complicated.
	//panic("todo")
    index := ms.ComputeNodeIndex(nodeAddr) 
	
	new_node := Node {
       Addr : nodeAddr,
	   Index : index, 
	}
    ms.Nodes = append (ms.Nodes, new_node)

	sort.Slice(ms.Nodes, func(i, j int) bool {
		if ms.Nodes[i].Index < ms.Nodes[j].Index {
			return true
		} else {
			return false
		}
	})

	for _, i := range (ms.Nodes) {
        fmt.Printf (i.Addr + strconv.Itoa (i.Index) + " ")
	}
	fmt.Printf ("\n")
}

// Remove the given nodeAddr from the ring.
func (ms *ConsistentHashRing) RemoveNode(nodeAddr string) {
	// O(N) solution is totally fine here.
	// O(log N) solution might be overly complicated.
	//panic("todo")
    index := ms.ComputeNodeIndex(nodeAddr) 
    for i, node := range (ms.Nodes) {
		if index == node.Index {
			ms.Nodes = append (ms.Nodes[:i], ms.Nodes[i+1:]...)
		}
	}
}

// Create consistent hash ring struct with a list of blockstore addresses
func NewConsistentHashRing(ringSize int, blockStoreAddrs []string) ConsistentHashRing {
	// You can not use ComputeNodeIndex method to compute the ring index of blockStoreAddr in blockStoreAddrs here.
	// You will need to use HashMod function, remember to hash the blockStoreAddr before calling HashMod
	// Hint: refer to ComputeNodeIndex method on how to hash the blockStoreAddr before calling HashMod
	//panic("todo")
	
	var nodes []Node
    
	for _, blockString := range (blockStoreAddrs) {

		hashBytes := sha256.Sum256([]byte(blockString))
		hashString := hex.EncodeToString(hashBytes[:])
		addressIndex := HashMod(hashString, ringSize)
		
        node := Node{
           Addr : blockString,
		   Index : addressIndex,
		}
		nodes = append (nodes, node)
		fmt.Printf (blockString + " " + strconv.Itoa(addressIndex) + "\n")
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Index < nodes[j].Index {
			return true
		} else {
			return false
		}
	})

	res := ConsistentHashRing {
		RingSize : ringSize,
		Nodes : nodes,
	}
    return res
}
