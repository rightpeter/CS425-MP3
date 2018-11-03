package model

// SIZE md5 size
const SIZE = 16

// RPCFile file structure for rpc
type RPCFile struct {
	Filename    string
	FileContent []byte
}

// RPCAddFileArgs args
type RPCAddFileArgs struct {
	Filename string
	MD5      [SIZE]byte
}

// RPCFilenameWithReplica reply
type RPCFilenameWithReplica struct {
	Filename    string
	ReplicaList []string
}

// RPCGetLatestVersionsArgs args
type RPCGetLatestVersionsArgs struct {
	Filename string
	Versions int
}

// RPCGetLatestVersionsReply reply
type RPCGetLatestVersionsReply struct {
	Filename    string
	Version     int
	ReplicaList []string
}

// RPCResult Result for rpc
type RPCResult struct {
	ClientID int
	Reply    string
	Alive    bool
	Error    error
}

// RPCPullFileFromArgs rpc pullfilefrom args
type RPCPullFileFromArgs struct {
	Filename string
	PullList []string
}

// NodeConfig Structure of node config
type NodeConfig struct {
	IP              string `json:"ip"`
	Port            int    `json:"port"`
	LogPath         string `json:"log_path"`
	FilePath        string `json:"file_path"`
	SleepTime       int    `json:"sleep_time"`        // Millisecond
	PullFileTimeout int    `json:"pull_file_timeout"` // Millisecond
}

// GlobalIndexFile contain maps which will give node->file and file->node mappings
// type GlobalIndexFile struct {
// 	Files map[string][]string
// 	Nodes map[string][]string
// }

type FileVersion struct {
	// nodes with that version
	Version int
	Nodes   []string
	Hash    [SIZE]byte
}

type FileStructure struct {
	Version  int
	Filename string
	Hash     [SIZE]byte
}

type GlobalIndexFile struct {
	// map from filename->latest md5 hash
	Filename map[string]FileStructure
	// map from Filename to different file versions
	Fileversions map[string][]FileVersion
	// map from node ID to list of files on the node
	NodesToFile map[string][]FileStructure
	// map from filename to list of nodes with the file
	FileToNodes map[string][]string
}

type PullInstruction struct {
	Filename string
	Node     string
	PullFrom []string // IDs with file
}
