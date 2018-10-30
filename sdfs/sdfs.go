// Package sdfs package for simple distributed file system
package sdfs

// refer to https://varshneyabhi.wordpress.com/2014/12/23/simple-udp-clientserver-in-golang/

import (
	"encoding/json"

	"CS425/CS425-MP1/model"
	failureDetector "CS425/CS425-MP2/server"
)

// SDFS SDFS class
type SDFS struct {
	config          model.NodeConfig
	sortedMemList   []string // ["id-ts", ...]
	failureDetector *failureDetector.Server
}

// NewSDFS init a SDFS
func NewSDFS(sdfsConfig []byte, failureDetectorConfig []byte) *SDFS {
	sdfs := &SDFS{}
	sdfs.init(sdfsConfig, failureDetectorConfig)
	return sdfs
}

// GetConfigPath export config file path
func (s *SDFS) GetConfigPath() string {
	return s.config.LogPath
}

func (s *SDFS) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &s.config)
}

func (s *SDFS) init(sdfsConfig []byte, failureDetectorConfig []byte) {
	s.failureDetector = failureDetector.NewServer(failureDetectorConfig)
	json.Unmarshal(sdfsConfig, &s.config)
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

// GetMemberList return sortedMemList
func (s *SDFS) GetMemberList() []string {
	return s.sortedMemList
}

// SetPort setport for SDFS
func (s *SDFS) SetPort(port int) {
	s.config.Port = port
}

// JoinToGroup jointogroup
func (s *SDFS) JoinToGroup() error {
	err := s.failureDetector.JoinToGroup()
	if err != nil {
		return err
	}
	return nil
}

// StartFailureDetector StartFailureDetector
func (s *SDFS) StartFailureDetector() {
	go s.failureDetector.ServerLoop()
	go s.failureDetector.FailureDetection()
	for {

	}
}
