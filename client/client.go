package main

import (
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
	content, err := ioutil.ReadFile(filename)
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

// func (c *Client) callDeleteFileRPC(client *rpc.Client, filename string) (string, error) {
// 	fmt.Println("filename: ", filename)
// 	var reply bool
// 	err := client.Call("Server.DeleteFile", &filename, &reply)
// 	if err != nil {
// 		fmt.Println(err)
// 		return "", err
// 	}
// 	return reply, nil
// }

// func (c *Client) callPushFileRPC(client *rpc.Client, filename string) (string, error) {
// 	fmt.Println("filename: ", filename)
// 	fileContent, err := c.readFileContent(filename)
// 	if err != nil {
// 		return "", err
// 	}
// 	args := &model.RPCPushFileArgs{
// 		Filename:    filename,
// 		FileContent: fileContent,
// 	}
// 	var reply string
// 	err = client.Call("Server.PushFile", args, &reply)
// 	if err != nil {
// 		fmt.Println(err)
// 		return "", err
// 	}
// 	return reply, nil
// }

// func (c *Client) callPullFileRPC(client *rpc.Client, filename string) error {
// 	fmt.Println("filename: ", filename)
// 	var reply model.RPCPushFileArgs
// 	err := client.Call("Server.PullFile", &filename, &reply)
// 	if err != nil {
// 		fmt.Println(err)
// 		return err
// 	}
// 	err = c.writeFile(reply.Filename, reply.FileContent)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (c *Client) pushFile(filename string) model.RPCResult {
// 	result := model.RPCResult{}
// 	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}

// 	reply, err := c.callPushFileRPC(client, filename)
// 	if err != nil {
// 		result.Error = err
// 		return result
// 	}
// 	result.Reply = reply
// 	fmt.Println("reply: ", reply)
// 	return result
// }

// func (c *Client) pullFile(filename string) model.RPCResult {
// 	result := model.RPCResult{}
// 	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}

// 	err = c.callPullFileRPC(client, filename)
// 	if err != nil {
// 		result.Error = err
// 		return result
// 	}
// 	return result
// }

// func (c *Client) deleteFile(filename string) error {
// 	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}

// 	ok, err = c.callDeleteFileRPC(client, filename)
// 	if err != nil {
// 		return err
// 	}
// 	if !ok {
// 		return errors.New("delete file failed")
// 	}
// 	return nil
// }

func (c *Client) callGetFileRPC(client *rpc.Client, filename string) (model.RPCFilenameWithReplica, error) {
	fmt.Println("filename: ", filename)
	var reply model.RPCFilenameWithReplica
	err := client.Call("Server.RPCGetFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFilenameWithReplica{}, err
	}
	return reply, nil
}

func (c *Client) callPullFileRPC(client *rpc.Client, filename string) (model.RPCFile, error) {
	fmt.Println("filename: ", filename)
	var reply model.RPCFile
	err := client.Call("Server.RPCPullFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFile{}, err
	}
	return reply, nil
}

func (c *Client) getFile(filename string) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	reply, err := c.callGetFileRPC(client, filename)
	if err != nil {
		return
	}

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
		file, err := c.callPullFileRPC(cl, filename)
		if err != nil {
			return
		}
		// TODO: could possible save in cache
		log.Printf("%s\n", file.FileContent)
		break
	}

}

func main() {
	configFile, e := ioutil.ReadFile("./config.json")
	if e != nil {
		log.Fatalf("File error: %v\n", e)
	}

	c := &Client{}
	c.loadConfigFromJSON(configFile)

	c.getFile("f1")

}
