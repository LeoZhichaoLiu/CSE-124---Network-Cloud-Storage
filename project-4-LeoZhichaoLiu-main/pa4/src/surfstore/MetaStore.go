package surfstore

import (
	"fmt"
	"net/rpc"
	"strconv"
)

type MetaStore struct {
	FileMetaMap    map[string]FileMetaData
	BlockStoreRing ConsistentHashRing
}

func (m *MetaStore) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	for k, v := range m.FileMetaMap {
		(*serverFileInfoMap)[k] = v
	}

	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	oldFileMeta, exist := m.FileMetaMap[fileMetaData.Filename]
	if !exist {
		// Create a dummy old file meta if the file does not exist yet
		oldFileMeta = FileMetaData{
			Filename:      "",
			Version:       0,
			BlockHashList: nil,
		}
	}

	// Compare the Version and decide to update or not. Should be exactly 1 greater
	if oldFileMeta.Version+1 == fileMetaData.Version {
		m.FileMetaMap[fileMetaData.Filename] = *fileMetaData
	} else {
		err = fmt.Errorf("Unexpected file Version. Yours:%d, Expected:%d, Lastest on Server:%d\n",
			fileMetaData.Version, oldFileMeta.Version+1, oldFileMeta.Version)
	}

	*latestVersion = m.FileMetaMap[fileMetaData.Filename].Version

	return
}

// Given an input hashlist, returns a mapping of BlockStore addresses to hashlists.
func (m *MetaStore) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// this should be different from your project 3 implementation. Now we have multiple
	// Blockstore servers instead of one Blockstore server in project 3. For each blockHash in
	// blockHashesIn, you want to find the BlockStore server it is in using consistent hash ring.
	//panic("todo")
        
	    fmt.Printf ("HAHAHAHAHA\n")

        // Loop for every node server
		for _, node := range (m.BlockStoreRing.Nodes) {
			// We set the hash array for each node server as empty array
			var hash_array [] string
		    fmt.Printf (node.Addr + "\n")
			(*blockStoreMap)[node.Addr] = hash_array
		}
        
		// Then loop for every hash, and put hash to its corresponding node
		for _, blockHash := range (blockHashesIn) {
            
			// We compute the index of block, and find the node that should store it
			block_index := m.BlockStoreRing.ComputeBlockIndex(blockHash)
            block_node := m.BlockStoreRing.FindHostingNode (block_index)
			
			// We then append the block into node's array.
			(*blockStoreMap)[block_node.Addr] = append ((*blockStoreMap)[block_node.Addr], blockHash)
		}     
		    
		return nil
}

// Add the specified BlockStore node to the cluster and migrate the blocks
func (m *MetaStore) AddNode(nodeAddr string, succ *bool) error {
	// compute node index
	index := m.BlockStoreRing.ComputeNodeIndex(nodeAddr)

	// find successor node (the node that need to release the block)
	node := m.BlockStoreRing.FindHostingNode(index)
	fmt.Printf (nodeAddr + " " + strconv.Itoa(index) + "\n")
	fmt.Printf ("Successor node: " + node.Addr + " " + strconv.Itoa(node.Index) + "\n")

	node_index := 0
	for i, n := range (m.BlockStoreRing.Nodes) {
		if n.Index == node.Index {
		   node_index = i
		}
	}
	// Find the previous nodes' index, the first ringindex should be it + 1.
	start_index := 0
	if node_index == 0 {
		start_index = m.BlockStoreRing.Nodes[len(m.BlockStoreRing.Nodes)-1].Index + 1
	} else {
		start_index = m.BlockStoreRing.Nodes[node_index-1].Index + 1
	}

	fmt.Printf ("start index" + strconv.Itoa(start_index) + "\n")
    
	// call RPC to migrate some blocks from successor node to this node
	inst := MigrationInstruction {
		LowerIndex: start_index,
		UpperIndex: index,
		DestAddr:   nodeAddr,
	}

	// connect to the server of successor node (release block)
	conn, e := rpc.DialHTTP("tcp", node.Addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("BlockStore.MigrateBlocks", inst, succ)
	if e != nil {
		conn.Close()
		return e
	}

	fmt.Printf ("Finishing migrating blocks\n")

	// deal with added node in BlockStoreRing
	//panic("todo")
	m.BlockStoreRing.AddNode(nodeAddr)

	// close the connection
	return conn.Close()
}

// Remove the specified BlockStore node from the cluster and migrate the blocks
func (m *MetaStore) RemoveNode(nodeAddr string, succ *bool) error {
	// compute node index
	index := m.BlockStoreRing.ComputeNodeIndex(nodeAddr)

	// find successor node (the node that will store removing block from current node)
	next_index := index + 1

	if index == m.BlockStoreRing.RingSize {
		next_index = 0
	}

	node := m.BlockStoreRing.FindHostingNode(next_index)
	node_index := 0
	for i, n := range (m.BlockStoreRing.Nodes) {
		if n.Index == index {
		   node_index = i
		}
	}
	// the start index of current node, is the previous node's index + 1.
	start_index := 0
	if node_index == 0 {
		start_index = m.BlockStoreRing.Nodes[len(m.BlockStoreRing.Nodes)-1].Index + 1
	} else {
		start_index = m.BlockStoreRing.Nodes[node_index-1].Index + 1
	}

	// call RPC to migrate all blocks from this node to successor 
	inst := MigrationInstruction {
		LowerIndex: start_index,
		UpperIndex: index,
		DestAddr:   node.Addr,  // The successor's node which will be merged into
	}

	// connect to the server
	conn, e := rpc.DialHTTP("tcp", nodeAddr) // The current node that will release block
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("BlockStore.MigrateBlocks", inst, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// deal with removed node in BlockStoreRing
	//panic("todo") 
	m.BlockStoreRing.RemoveNode(nodeAddr)

	// close the connection
	return conn.Close()
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreRing ConsistentHashRing) MetaStore {
	return MetaStore{
		FileMetaMap:    map[string]FileMetaData{},
		BlockStoreRing: blockStoreRing,
	}
}
