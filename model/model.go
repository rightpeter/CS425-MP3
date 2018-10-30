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

// GlobalIndexFile contain maps which will give node->file and file->node mappings
type GlobalIndexFile struct {
	Files map[string][]string
	Nodes map[string][]string
}
