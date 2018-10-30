package model

// RPCPushFileArgs for push file
type RPCPushFileArgs struct {
	Filename    string
	FileContent []byte
}

type RPCPushFileResult struct {
	Reply string
}

// RPCArgs Input Arguments for rpc
type RPCArgs struct {
	Commands []string
}

// RPCResult Result for rpc
type RPCResult struct {
	ClientID int
	Reply    string
	Alive    bool
	Error    error
}

// NodeConfig Structure of node config
type NodeConfig struct {
	ID      int    `json:"id"`
	IP      string `json:"ip"`
	Port    int    `json:"port"`
	LogPath string `json:"log_path"`
}

// NodesConfig structure to unmarshal json config file {id: int, ip: string, port: int}
type NodesConfig struct {
	Current NodeConfig   `json:"current"`
	Nodes   []NodeConfig `json:"nodes"`
}

// FileIndex will store filename and IPs of machines with that file
type FileIndex struct {
	FileName []string
}

// NodeIndex will store machine IP and filenames on that machine
type NodeIndex struct {
	IP []string
}

// GlobalIndexFile contain node and file indexes. Will be same across all alive processes
type GlobalIndexFile struct {
	Files []FileIndex
	Nodes []NodeIndex
}
