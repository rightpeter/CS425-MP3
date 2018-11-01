// Package sdfs package for simple distributed file system
package sdfs

// refer to https://varshneyabhi.wordpress.com/2014/12/23/simple-udp-clientserver-in-golang/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"

	failureDetector "CS425/CS425-MP2/server"
	SDFSIndex "CS425/CS425-MP3/index"
	"CS425/CS425-MP3/model"
)

// SDFS SDFS class
type SDFS struct {
	config          model.NodeConfig
	sortedMemList   []string // ["id-ts", ...]
	nodesRPCClients map[string]*rpc.Client
	failureDetector *failureDetector.Server
	master          string
	id              string
	filePath        string
	index           SDFSIndex.Index
}

// NewSDFS init a SDFS
func NewSDFS(sdfsConfig []byte, failureDetectorConfig []byte) *SDFS {
	sdfs := &SDFS{}
	sdfs.init(sdfsConfig, failureDetectorConfig)
	return sdfs
}

func (s *SDFS) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &s.config)
}

func (s *SDFS) init(sdfsConfig []byte, failureDetectorConfig []byte) {
	s.failureDetector = failureDetector.NewServer(failureDetectorConfig)
	json.Unmarshal(sdfsConfig, &s.config)
	s.filePath = s.config.FilePath
	s.id = s.failureDetector.GetID()
	s.master = s.id
}

func (s *SDFS) reElect() error {
	s.master = s.sortedMemList[0]
	return nil
}

// InitIndex init the index
func (s *SDFS) InitIndex() error {
	s.sortedMemList = s.failureDetector.GetMemberList()
	if s.isMaster() {
		err := s.pullIndex(s.sortedMemList[1])
		if err != nil {
			return err
		}
	} else {
		err := s.pullIndex(s.master)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetLogPath export config file path
func (s *SDFS) GetLogPath() string {
	return s.config.LogPath
}

func (s *SDFS) pullIndex(nodeID string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	globalIndex := &model.GlobalIndexFile{}
	err = client.Call("SDFS.RPCPullIndex", &nodeID, &globalIndex)
	if err != nil {
		return err
	}

	s.index = SDFSIndex.LoadFromIndex(*globalIndex)
	return nil
}

func (s *SDFS) pushIndex(nodeID string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	globalIndex := s.index.GetGlobalIndexFile()
	var ok bool
	err = client.Call("SDFS.RPCPushIndex", &globalIndex, &ok)
	if err != nil {
		return err
	}
	return nil
}

// GetIP getip for server
func (s *SDFS) GetIP() string {
	return s.config.IP
}

// SetIP setip for SDFS
func (s *SDFS) SetIP(IP string) {
	s.config.IP = IP
}

// GetPort getport of SDFS
func (s *SDFS) GetPort() int {
	return s.config.Port
}

func (s *SDFS) getIPFromID(nodeID string) string {
	return strings.Split(nodeID, "-")[0]
}

func (s *SDFS) isMaster() bool {
	return s.id == s.master
}

func (s *SDFS) getRPCClient(nodeID string) (*rpc.Client, error) {
	client := &rpc.Client{}
	if client, ok := s.nodesRPCClients[nodeID]; !ok {
		return nil, fmt.Errorf("no rpc client for node: %v", nodeID)
	}
	return client, nil
}

func (s *SDFS) updateMemberList() ([]string, []string) {
	newMemList := s.failureDetector.GetMemberList()
	reElect := false
	if s.sortedMemList[0] != newMemList[0] {
		reElect = true
	}

	newNodeList := []string{}
	failNodeList := []string{}
	i := 0
	j := 0

	for i < len(s.sortedMemList) && j < len(newMemList) {
		if s.sortedMemList[i] < newMemList[j] {
			failNodeList = append(failNodeList, s.sortedMemList[i])
			i++
		}
		if s.sortedMemList[i] == newMemList[j] {
			i++
			j++
		}
		if s.sortedMemList[i] > newMemList[j] {
			newNodeList = append(newNodeList, newMemList[j])
			j++
		}
	}

	for i < len(s.sortedMemList) {
		failNodeList = append(failNodeList, s.sortedMemList[i])
		i++
	}

	for j < len(newMemList) {
		newNodeList = append(newNodeList, newMemList[j])
		j++
	}

	s.sortedMemList = newMemList

	if reElect {
		s.reElect()
	}

	return newNodeList, failNodeList
}

func (s *SDFS) updateNewNodes(newNodes []string) []string {
	failNodes := []string{}
	for _, node := range newNodes {
		s.index.AddNode(node)
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", s.getIPFromID(node), s.config.Port))
		if err != nil {
			log.Printf("updateMemberList: rpc.DialHTTP failed")
			failNodes = append(failNodes, node)
		}
		s.nodesRPCClients[node] = client
	}
	return failNodes
}

func (s *SDFS) updateFailNodes(failNodes []string) []string {
	failFailNodes := []string{}
	for _, node := range failNodes {
		pullList := s.index.RemoveNode(node)
		for _, pull := range pullList {
			err := s.askNodeToPullFileFromNode(pull.Filename, pull.Node, pull.PullFrom)
			if err != nil {
				log.Printf("updateFailNodes: ask %v pull file: %v from list: %v failed", pull.Node, pull.Filename, pull.PullFrom)
				failFailNodes = append(failFailNodes, node)
			}
		}
		delete(s.nodesRPCClients, node)
	}
	return failFailNodes
}

//UpdateMemberList updatememberlist loop
func (s *SDFS) UpdateMemberList() {
	for {
		time.Sleep(time.Duration(s.config.SleepTime) * time.Millisecond)
		failNodes, newNodes := s.updateMemberList()
		if s.isMaster() {
			s.updateNewNodes(newNodes)
			s.updateFailNodes(failNodes)
		}
	}
}

// GetMemberList return sortedMemList
func (s *SDFS) GetMemberList() []string {
	s.updateMemberList()
	return s.sortedMemList
}

// SetPort setport for SDFS
func (s *SDFS) SetPort(port int) {
	s.config.Port = port
}

// StartFailureDetector StartFailureDetector
func (s *SDFS) StartFailureDetector() {
	for {
		err := s.failureDetector.JoinToGroup()
		if err != nil {
			log.Printf("join to group failed: %s\n", err.Error())
			log.Printf("try to join to group 5 seconds later...")
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("join to group successfully\n\n")
		break
	}

	log.Printf("Starting server on IP: %s and port: %d\n\n", s.failureDetector.GetIP(), s.failureDetector.GetPort())
	go s.failureDetector.ServerLoop()
	s.failureDetector.FailureDetection()
}

func (s *SDFS) writeFile(filename string, fileContent []byte) error {
	err := ioutil.WriteFile(s.filePath+filename, fileContent, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (s *SDFS) readFileContent(filename string) ([]byte, error) {
	content, err := ioutil.ReadFile(s.filePath + filename)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (s *SDFS) deleteFile(filename string) error {
	err := os.Remove(s.filePath + filename)
	if err != nil {
		return err
	}
	return nil
}

// RPCDeleteFile RPC to delete file
func (s *SDFS) RPCDeleteFile(filename *string, ok *bool) error {
	err := s.deleteFile(*filename)
	if err != nil {
		*ok = false
		return err
	}
	*ok = true
	return nil
}

// RPCPutFile RPC to add file
func (s *SDFS) RPCPutFile(file *model.RPCAddFileArgs, reply *model.RPCFilenameWithReplica) error {
	filename, replicaList, err := s.index.AddFile(file.Filename, file.MD5)
	if err != nil {
		return err
	}

	reply = &model.RPCFilenameWithReplica{
		Filename:    filename,
		ReplicaList: replicaList,
	}
	return nil
}

// RPCGetFile RPC to get file
func (s *SDFS) RPCGetFile(filename *string, reply *model.RPCFilenameWithReplica) error {
	version, replicaList, err := s.index.GetFile(*filename)
	if err != nil {
		return err
	}

	reply = &model.RPCFilenameWithReplica{
		Filename:    fmt.Sprintf("%s_%d", *filename, version),
		ReplicaList: replicaList,
	}
	return nil
}

// RPCGetLatestVersions RPC to get latest versions of file
func (s *SDFS) RPCGetLatestVersions(args *model.RPCGetLatestVersionsArgs, reply []*model.RPCGetLatestVersionsReply) error {
	fileList, err := s.index.GetLatestVersions(args.Filename, args.Versions)
	if err != nil {
		return err
	}

	reply = []*model.RPCGetLatestVersionsReply{}
	for _, file := range fileList {
		reply = append(reply, &model.RPCGetLatestVersionsReply{
			Filename:    file.Filename,
			ReplicaList: file.ReplicaList,
		})
	}
	return nil
}

// RPCPushFile RPC
func (s *SDFS) RPCPushFile(file *model.RPCFile, ok *bool) error {
	err := s.writeFile(file.Filename, file.FileContent)
	if err != nil {
		*ok = false
		return err
	}
	*ok = true
	return nil
}

//RPCPullIndex RPC
func (s *SDFS) RPCPullIndex(nodeID *string, index *model.GlobalIndexFile) error {
	*index = s.index.GetGlobalIndexFile()
	return nil
}

//RPCPushIndex RPC
func (s *SDFS) RPCPushIndex(globalIndex *model.GlobalIndexFile, ok *bool) error {
	s.index = SDFSIndex.LoadFromIndex(globalIndex)
	*ok = true
}

// RPCPullFile RPC
func (s *SDFS) RPCPullFile(filename *string, replyFile *model.RPCFile) error {
	fmt.Println("File: ", *filename)
	fileContent, err := s.readFileContent(*filename)
	if err != nil {
		return err
	}
	replyFile.FileContent = fileContent
	replyFile.Filename = *filename
	return err
}

// RPCPullFileFrom RPC
func (s *SDFS) RPCPullFileFrom(args *model.RPCPullFileFromArgs, ok *bool) error {
	ch := make(chan []byte)
	for _, nodeID := range args.PullList {
		go func(nodeID string) {
			select {
			case ch <- s.pullFileFromNode(args.Filename, nodeID):
			case <-time.After(time.Duration(s.config.PullFileTimeout) * time.Millisecond):
				ch <- nil
			}
		}(nodeID)
	}

	fileContent := nil
	i := len(args.PullList) - 1
	// get first response
	for fileContent == nil && i >= 0 {
		fileContent = <-ch
	}

	if fileContent == nil {
		*ok = false
		return err
	}

	err = s.writeFile(args.Filename, fileContent)
	if err != nil {
		*ok = false
		return err
	}

	*ok = true
	return nil
}

func (s *SDFS) pushFileToNode(filename string, nodeID string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	fileContent, err := c.readFileContent(filename)
	if err != nil {
		return err
	}

	args := &model.RPCFile{
		Filename:    filename,
		FileContent: fileContent,
	}

	var ok bool
	err = client.Call("SDFS.RPCPushFile", args, &ok)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("push file to %s failed", nodeID)
	}
	return nil
}

func (s *SDFS) pullFileFromNode(filename string, nodeID string) []byte {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		log.Printf("pullFileFromNode: get rpc client of %v failed", nodeID)
		return nil
	}

	var file model.RPCFile
	err = client.Call("Server.RPCPullFile", &filename, &file)
	if err != nil {
		log.Printf("pullFileFromNode: pull %v from %v failed", filename, nodeID)
		return nil
	}

	return reply.FileContent
}

func (s *SDFS) askNodeToPullFileFromNode(filename string, nodeID string, pullNodeList []string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	args := &model.RPCPullFileFromArgs{
		Filename: filename,
		NodeID:   targetNodeID,
	}

	var ok bool
	err = client.Call("Server.RPCPullFileFrom", &args, &ok)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("ask %v to pull file(%v) from %v failed", nodeID, filename, targetNodeID)
	}

	return nil
}
func (s *SDFS) deleteFileOnNode(filename string, nodeID string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	var ok bool
	err = client.Call("Server.DeleteFile", &filename, &ok)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("delete file on %s failed", nodeID)
	}
	return nil
}
