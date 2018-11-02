package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"strings"

	"CS425/CS425-MP3/model"

	"encoding/json"
)

// Client struct
type Client struct {
	config model.NodeConfig
}

func (c *Client) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &c.config)
}

func (c *Client) readFileContent(filename string) ([]byte, error) {
	content, err := ioutil.ReadFile("./files/" + filename)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (c *Client) writeFile(filename string, fileContent []byte) error {
	err := ioutil.WriteFile(filename, fileContent, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) getIPFromID(nodeID string) string {
	return strings.Split(nodeID, "-")[0]
}

func (c *Client) callGetFileRPC(client *rpc.Client, filename string) (model.RPCFilenameWithReplica, error) {
	// fmt.Println("filename: ", filename)
	var reply model.RPCFilenameWithReplica
	err := client.Call("SDFS.RPCGetFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFilenameWithReplica{}, err
	}
	return reply, nil
}

func (c *Client) callPullFileRPC(client *rpc.Client, filename string) (model.RPCFile, error) {
	// fmt.Println("filename: ", filename)
	var reply model.RPCFile
	err := client.Call("SDFS.RPCPullFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFile{}, err
	}
	return reply, nil
}

func (c *Client) callDeleteFileRPC(client *rpc.Client, filename string) (bool, error) {
	// fmt.Println("filename: ", filename)
	var reply bool
	err := client.Call("SDFS.RPCDeleteFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	return reply, nil
}

func (c *Client) pushFileToNode(filename string, filenameVersion string, nodeID string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(nodeID), c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	fileContent, err := c.readFileContent(filename)
	if err != nil {
		return err
	}

	args := model.RPCFile{
		Filename:    filenameVersion,
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

func (c *Client) callPutFileRPC(client *rpc.Client, filename string) (model.RPCFilenameWithReplica, error) {
	var reply model.RPCFilenameWithReplica
	fileContent, err := c.readFileContent(filename)
	if err != nil {
		return model.RPCFilenameWithReplica{}, err
	}
	file := model.RPCAddFileArgs{
		Filename: filename,
		MD5:      md5.Sum(fileContent),
	}
	err = client.Call("SDFS.RPCPutFile", &file, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFilenameWithReplica{}, err
	}
	fmt.Printf("Replica list: %v/n", reply.ReplicaList)
	for _, nID := range reply.ReplicaList {
		fmt.Printf("Pushing file %s to %v", reply.Filename, nID)
		c.pushFileToNode(filename, reply.Filename, nID)
	}
	return reply, nil
}

func (c *Client) putFile(filename string) {
	fmt.Println("putFile: ", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println("Connection made")

	reply, err := c.callPutFileRPC(client, filename)
	if err != nil {
		return
	}

	log.Printf("%s is on %v", filename, reply.ReplicaList)
}

func (c *Client) getFile(filename string) {
	fmt.Println("getFile: ", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println("Connection made")
	reply, err := c.callGetFileRPC(client, filename)
	if err != nil {
		return
	}
	fmt.Printf("GETFILE: replicalist %v", reply.ReplicaList)

	fmt.Printf("Files with %s: %v \n", filename, reply.ReplicaList)

	if len(reply.ReplicaList) == 0 {
		log.Println("File not available")
		return
	}

	log.Printf("Nodes with FileName: %v \n", reply.ReplicaList)

	for _, id := range reply.ReplicaList {
		cl, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(id), c.config.Port))
		if err != nil {
			log.Fatal("dialing:", err)
		}
		// TODO: Could possible use a goroutine
		file, err := c.callPullFileRPC(cl, reply.Filename)
		if err != nil {
			return
		}
		// TODO: could possible save in cache
		fmt.Printf("Content:\n%s\n", file.FileContent)
		break
	}
}

// func (c *Client) putFile(filename string) {
// 	fmt.Println("putFile: ", filename)
// 	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	fmt.Println("Connection made")

// 	reply, err := c.callPutFileRPC(client, filename)
// 	if err != nil {
// 		return
// 	}

// 	log.Printf("%s is on %v", filename, reply.ReplicaList)
// }

func (c *Client) deleteFile(filename string) {
	fmt.Println("deleteFile: ", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	reply, err := c.callGetFileRPC(client, filename)
	if err != nil {
		return
	}

	fmt.Printf("deleteFile: Files with %s: %v \n", filename, reply.ReplicaList)

	if len(reply.ReplicaList) == 0 {
		log.Println("File not available")
		return
	}

	log.Printf("Nodes with FileName: %v \n", reply.ReplicaList)

	for _, id := range reply.ReplicaList {
		cl, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(id), c.config.Port))
		if err != nil {
			log.Fatal("dialing:", err)
		}
		// TODO: Could possible use a goroutine
		del, err := c.callDeleteFileRPC(cl, reply.Filename)
		if err != nil {
			return
		}
		if del {
			fmt.Printf("Deleted: %s\n", reply.Filename)
		} else {
			fmt.Printf("Failed to delete: %s\n", reply.Filename)
		}

	}

}

func main() {
	configFile, e := ioutil.ReadFile("./config.json")
	if e != nil {
		log.Fatalf("File error: %v\n", e)
	}
	c := &Client{}
	c.loadConfigFromJSON(configFile)

	getFilename := flag.String("get", "", "get {filename}")

	putFilename := flag.String("put", "", "put {filename}")

	flag.Parse()

	if *getFilename != "" {
		c.getFile(*getFilename)
	} else if *putFilename != "" {
		c.putFile(*putFilename)
	}

}
