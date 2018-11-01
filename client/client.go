package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	"CS425/CS425-MP3/model"

	"encoding/json"
)

// Client struct
type Client struct {
	config model.NodesConfig
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

func (c *Client) callDeleteFileRPC(client *rpc.Client, filename string) (string, error) {
	fmt.Println("filename: ", filename)
	var reply bool
	err := client.Call("Server.DeleteFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return reply, nil
}

func (c *Client) callPushFileRPC(client *rpc.Client, filename string) (string, error) {
	fmt.Println("filename: ", filename)
	fileContent, err := c.readFileContent(filename)
	if err != nil {
		return "", err
	}
	args := &model.RPCPushFileArgs{
		Filename:    filename,
		FileContent: fileContent,
	}
	var reply string
	err = client.Call("Server.PushFile", args, &reply)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return reply, nil
}

func (c *Client) callPullFileRPC(client *rpc.Client, filename string) error {
	fmt.Println("filename: ", filename)
	var reply model.RPCPushFileArgs
	err := client.Call("Server.PullFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = c.writeFile(reply.Filename, reply.FileContent)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) pushFile(filename string) model.RPCResult {
	result := model.RPCResult{}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	reply, err := c.callPushFileRPC(client, filename)
	if err != nil {
		result.Error = err
		return result
	}
	result.Reply = reply
	fmt.Println("reply: ", reply)
	return result
}

func (c *Client) pullFile(filename string) model.RPCResult {
	result := model.RPCResult{}
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	err = c.callPullFileRPC(client, filename)
	if err != nil {
		result.Error = err
		return result
	}
	return result
}

func (c *Client) deleteFile(filename string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", "0.0.0.0", 8080))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	ok, err = c.callDeleteFileRPC(client, filename)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("delete file failed")
	}
	return nil
}

func main() {
	// configFile, e := ioutil.ReadFile("./config.json")
	// if e != nil {
	// 	log.Fatalf("File error: %v\n", e)
	// }

	c := &Client{}
	// c.loadConfigFromJSON(configFile)

	c.pushFile(os.Args[1])
	c.pullFile("pull.txt")
	c.deleteFile("del.txt")

}
