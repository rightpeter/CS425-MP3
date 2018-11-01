package filemanagement

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

	"CS425/CS425-MP3/model"
)

// Server Server Struct
type Server struct {
	config model.NodesConfig
}

func newServer() *Server {
	return &Server{}
}

func (s *Server) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &s.config)
}

func (s *Server) getIP() string {
	return s.config.Current.IP
}

func (s *Server) setIP(IP string) {
	s.config.Current.IP = IP
}

func (s *Server) getPort() int {
	return s.config.Current.Port
}

func (s *Server) setPort(port int) {
	s.config.Current.Port = port
}

func (s *Server) getFilePath() string {
	return s.config.Current.LogPath
}

func (s *Server) setFilePath(path string) {
	s.config.Current.LogPath = path
}

func (s *Server) writeFile(filename string, fileContent []byte) error {
	err := ioutil.WriteFile(filename, fileContent, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) readFileContent(filename string) ([]byte, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (s *Server) deleteFile(filename string) error {
	err := os.Remove(filename)
	if err != nil {
		return nil
	}
	return nil
}

// DeleteFile RPC to delete file
func (s *Server) DeleteFile(filename *string, reply *string) error {
	err := s.deleteFile(*filename)
	if err != nil {
		return err
	}
	*reply = "success"
	return err
}

// PushFile RPC
func (s *Server) PushFile(args *model.RPCPushFileArgs, reply *string) error {
	fmt.Println("File: ", args.Filename)
	fmt.Println("Content: ", args.FileContent)
	err := s.writeFile(args.Filename, args.FileContent)
	if err != nil {
		return err
	}
	*reply = "success"
	return err
}

// PullFile RPC
func (s *Server) PullFile(filename *string, reply *model.RPCPushFileArgs) error {
	fmt.Println("File: ", *filename)
	fileContent, err := s.readFileContent(*filename)
	if err != nil {
		return err
	}
	reply.FileContent = fileContent
	reply.Filename = *filename
	return err
}

// This function will register and initiate server
func main() {

	// parse argument
	// configFilePath := flag.String("c", "./config.json", "Config file path")
	port := flag.Int("p", 8080, "Port number")
	IP := flag.String("ip", "0.0.0.0", "IP address")

	// flag.Parse()

	// // load config file
	// configFile, e := ioutil.ReadFile(*configFilePath)
	// if e != nil {
	// 	log.Fatalf("File error: %v\n", e)
	// }

	// Class for rpc
	server := newServer()

	server.setIP(*IP)
	server.setPort(*port)

	fmt.Printf("Starting server on IP: %s and port: %d", *IP, *port)

	// init the rpc server
	rpc.Register(server)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", server.getPort()))
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	http.Serve(l, nil)
}
