// Package sdfs package for simple distributed file system
package main

// refer to https://varshneyabhi.wordpress.com/2014/12/23/simple-udp-clientserver-in-golang/

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
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
	s.nodesRPCClients = map[string]*rpc.Client{}
}

func (s *SDFS) reElect() error {
	s.master = s.sortedMemList[0]
	return nil
}

func (s *SDFS) initIndex() error {
	s.sortedMemList = s.failureDetector.GetMemberList()
	if s.isMaster() {
		if len(s.sortedMemList) > 1 {
			err := s.pullIndex(s.sortedMemList[1])
			if err != nil {
				return err
			}
		} else {
			s.index = SDFSIndex.NewIndex()
		}
	} else {
		err := s.pullIndex(s.master)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SDFS) getLogPath() string {
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

	s.index = SDFSIndex.LoadFromGlobalIndexFile(*globalIndex)
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

func (s *SDFS) pushIndexToAll() []string {
	failList := []string{}
	for _, node := range s.sortedMemList {
		if node != s.id {
			err := s.pushIndex(node)
			if err != nil {
				failList = append(failList, node)
			}
		}
	}
	return failList
}

func (s *SDFS) getIP() string {
	return s.config.IP
}

func (s *SDFS) setIP(IP string) {
	s.config.IP = IP
}

func (s *SDFS) getPort() int {
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
	ok := false
	if client, ok = s.nodesRPCClients[nodeID]; !ok {
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
		if i < len(s.sortedMemList) && j < len(newMemList) && s.sortedMemList[i] > newMemList[j] {
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
		s.index.AddNewNode(node)
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

func (s *SDFS) keepUpdatingMemberList() {
	for {
		time.Sleep(time.Duration(s.config.SleepTime) * time.Millisecond)
		failNodes, newNodes := s.updateMemberList()
		log.Printf("keepUpdatingMemberList: memberList: %v", s.sortedMemList)
		if s.isMaster() {
			s.updateNewNodes(newNodes)
			s.updateFailNodes(failNodes)
			log.Printf("keepUpdatingMemberList: updated newNodes: %v, failNodes: %v", newNodes, failNodes)
			s.pushIndexToAll()
		}
	}
}

func (s *SDFS) getMemberList() []string {
	return s.sortedMemList
}

func (s *SDFS) setPort(port int) {
	s.config.Port = port
}

func (s *SDFS) startFailureDetector() {
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
	if s.isMaster() {
		version, replicaList := s.index.AddFile(file.Filename, file.MD5)

		failList := s.pushIndexToAll()
		if len(failList) > 0 {
			log.Printf("Push Index to nodes: %v failed", failList)
		}
		reply = &model.RPCFilenameWithReplica{
			Filename:    fmt.Sprintf("%s_%d", file.Filename, version),
			ReplicaList: replicaList,
		}
	} else {
		err := s.putFile(file, reply)
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCGetFile RPC to get file
func (s *SDFS) RPCGetFile(filename *string, reply *model.RPCFilenameWithReplica) error {
	version, replicaList := s.index.GetFile(*filename)

	reply = &model.RPCFilenameWithReplica{
		Filename:    fmt.Sprintf("%s_%d", *filename, version),
		ReplicaList: replicaList,
	}
	return nil
}

// RPCLs RPC to get file
func (s *SDFS) RPCLs(filename *string, reply *[]string) error {
	_, replicaList := s.index.GetFile(*filename)

	*reply = replicaList
	return nil
}

// RPCGetLatestVersions RPC to get latest versions of file
func (s *SDFS) RPCGetLatestVersions(args *model.RPCGetLatestVersionsArgs, reply *[]model.RPCGetLatestVersionsReply) error {
	fileList := s.index.GetVersions(args.Filename, args.Versions)

	tmpReply := []model.RPCGetLatestVersionsReply{}
	for _, file := range fileList {
		tmpReply = append(tmpReply, model.RPCGetLatestVersionsReply{
			Filename:    args.Filename,
			ReplicaList: file.Nodes,
		})
	}
	*reply = tmpReply
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
	s.index = SDFSIndex.LoadFromGlobalIndexFile(*globalIndex)
	*ok = true
	return nil
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

	var fileContent []byte
	i := len(args.PullList) - 1
	// get first response
	for fileContent == nil && i >= 0 {
		fileContent = <-ch
	}

	if fileContent == nil {
		*ok = false
		return fmt.Errorf("RPCPullFileFrom: pull file failed")
	}

	err := s.writeFile(args.Filename, fileContent)
	if err != nil {
		*ok = false
		return err
	}

	*ok = true
	return nil
}

func (s *SDFS) putFile(file *model.RPCAddFileArgs, reply *model.RPCFilenameWithReplica) error {
	client, err := s.getRPCClient(s.master)
	if err != nil {
		return err
	}

	err = client.Call("SDFS.RPCPutFile", file, reply)
	if err != nil {
		return err
	}
	return nil
}

func (s *SDFS) pushFileToNode(filename string, nodeID string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	fileContent, err := s.readFileContent(filename)
	if err != nil {
		return err
	}

	args := model.RPCFile{
		Filename:    filename,
		FileContent: fileContent,
	}

	var ok bool
	err = client.Call("SDFS.RPCPushFile", &args, &ok)
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

	return file.FileContent
}

func (s *SDFS) askNodeToPullFileFromNode(filename string, nodeID string, pullNodeList []string) error {
	client, err := s.getRPCClient(nodeID)
	if err != nil {
		return err
	}

	args := &model.RPCPullFileFromArgs{
		Filename: filename,
		PullList: pullNodeList,
	}

	var ok bool
	err = client.Call("Server.RPCPullFileFrom", &args, &ok)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("ask %v to pull file(%v) from %v failed", nodeID, filename, pullNodeList)
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

// This function will register and initiate server
func main() {
	// parse argument
	configFilePath := flag.String("c", "./config.json", "Config file path")
	fdConfigFilePath := flag.String("fdc", "./failure_detector.config.json", "Failure Detector Config file path")

	// load config file
	configFile, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	// load fd config file
	fdConfigFile, err := ioutil.ReadFile(*fdConfigFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	// Class for server
	s := NewSDFS(configFile, fdConfigFile)

	f, err := os.OpenFile(s.getLogPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	go s.startFailureDetector()
	go s.keepUpdatingMemberList()
	err = s.initIndex()
	if err != nil {
		log.Printf("main: Index init failed")
	}

	// init the rpc server
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", s.getPort()))
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	log.Printf("Start listen rpc on port: %d", s.getPort())
	http.Serve(l, nil)
}
