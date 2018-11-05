package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

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

func (c *Client) callRemoveFileRPC(client *rpc.Client, filename string) ([]string, error) {
	// fmt.Println("filename: ", filename)
	var reply []string
	err := client.Call("SDFS.RPCRemoveFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return reply, nil
}

func (c *Client) callPullFileRPC(client *rpc.Client, filename string) (model.RPCFile, error) {
	var reply model.RPCFile
	err := client.Call("SDFS.RPCPullFile", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return model.RPCFile{}, err
	}
	return reply, nil
}

func (c *Client) callDeleteFileStarRPC(client *rpc.Client, filename string) (bool, error) {
	var reply bool
	err := client.Call("SDFS.RPCDeleteFileStar", &filename, &reply)
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	return reply, nil
}

func (c *Client) callGetVersionsRPC(client *rpc.Client, filename string, numVersions int) ([]model.RPCGetLatestVersionsReply, error) {
	args := model.RPCGetLatestVersionsArgs{
		Filename: filename,
		Versions: numVersions,
	}
	var reply []model.RPCGetLatestVersionsReply
	err := client.Call("SDFS.RPCGetLatestVersions", &args, &reply)
	if err != nil {
		fmt.Println(err)
		return []model.RPCGetLatestVersionsReply{}, err
	}
	return reply, nil
}

func (c *Client) pushFileToNode(filename string, filenameVersion string, nodeID string) error {
	fmt.Printf("pushFileToNode: DialHTTP: ip: %s, port: %d\n", c.getIPFromID(nodeID), c.config.Port)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(nodeID), c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	fmt.Printf("pushFileToNode: readFileContent: %v\n", filename)
	fileContent, err := c.readFileContent(filename)
	if err != nil {
		return err
	}

	args := model.RPCFile{
		Filename:    filenameVersion,
		FileContent: fileContent,
	}

	var ok bool
	fmt.Printf("pushFileToNode: calling SDFS.RPCPushFile")
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
	fmt.Printf("Replica list: %v\n", reply.ReplicaList)
	for _, nID := range reply.ReplicaList {
		fmt.Printf("Pushing file %s to %v", reply.Filename, nID)
		c.pushFileToNode(filename, reply.Filename, nID)
	}
	return reply, nil
}

func (c *Client) callLsRPC(client *rpc.Client, filename string) ([]string, error) {
	replicaList := []string{}
	err := client.Call("SDFS.RPCLsReplicasOfFile", &filename, &replicaList)
	if err != nil {
		return nil, err
	}

	return replicaList, nil
}

func (c *Client) callStoresRPC(client *rpc.Client, nodeID string) ([]string, error) {
	fileList := []string{}
	err := client.Call("SDFS.RPCStoresOnNode", &nodeID, &fileList)
	if err != nil {
		return nil, err
	}

	return fileList, nil
}

func (c *Client) putFile(filename string) {
	t0 := time.Now()
	fmt.Println("putFile: ", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println("Connection made")

	reply, err := c.callPutFileRPC(client, filename)
	if err != nil {
		fmt.Printf("Time for -put: %v\n", time.Since(t0))
		return
	}

	log.Printf("%s is on %v\n", filename, reply.ReplicaList)
	fmt.Printf("Time for -put: %v\n", time.Since(t0))
}

func (c *Client) putFolder(folder string) {
	t0 := time.Now()

	fmt.Println("putFolder: ", folder)

	files, err := ioutil.ReadDir("./files/" + folder)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		filename := f.Name()
		c.putFile(filename)
		fmt.Printf("Push %s finished!", filename)
	}

	fmt.Printf("Time for -put-folder: %v\n", time.Since(t0))
}

func (c *Client) getFile(filename string) {
	fmt.Println("getFile: ", filename)
	t0 := time.Now()
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	fmt.Println("Connection made")
	reply, err := c.callGetFileRPC(client, filename)
	if err != nil {
		return
	}
	fmt.Printf("GETFILE: replicalist %v\n", reply.ReplicaList)

	fmt.Printf("Files with %s: %v \n", filename, reply.ReplicaList)

	if len(reply.ReplicaList) == 0 {
		log.Println("File not available")
		fmt.Printf("Time for -get: %v\n", time.Since(t0))
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
		fmt.Printf("Saved in files folder:\n%s\n", file.FileContent)
		c.writeFile("./fetched_files/"+filename, file.FileContent)
		fmt.Printf("Time for -get: %v\n", time.Since(t0))
		break
	}
	fmt.Printf("Time for -get: %v\n", time.Since(t0))
}

func (c *Client) getFileFromNode(filename string, nodeID string) []byte {
	cl, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(nodeID), c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	file, err := c.callPullFileRPC(cl, filename)
	if err != nil {
		return nil
	}
	return file.FileContent
}

func (c *Client) deleteFile(filename string) {
	t0 := time.Now()
	fmt.Println("deleteFile: ", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	reply, err := c.callRemoveFileRPC(client, filename)
	if err != nil {
		return
	}

	fmt.Printf("deleteFile: Files with %s: %v \n", filename, reply)

	if len(reply) == 0 {
		log.Println("File not available")
		return
	}

	log.Printf("Nodes with FileName: %v \n", reply)

	for _, id := range reply {
		cl, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.getIPFromID(id), c.config.Port))
		if err != nil {
			log.Fatal("dialing:", err)
		}
		// TODO: Could possible use a goroutine
		del, err := c.callDeleteFileStarRPC(cl, filename)
		if err != nil {
			return
		}
		if del {
			fmt.Printf("Deleted: %s\n", filename)
		} else {
			fmt.Printf("Failed to delete: %s\n", filename)
		}
	}
	fmt.Printf("Time for -del: %v\n", time.Since(t0))
}

func (c *Client) getVersionForFile(filename string, numVersions int, outFileName string) {

	table := make(map[string]int)
	fmt.Printf("filename: %s, versions: %d", filename, numVersions)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	reply, err := c.callGetVersionsRPC(client, filename, numVersions)
	if err != nil {
		return
	}

	fmt.Printf("getVersionForFile: Files with %s: %v \n", filename, reply)

	if len(reply) == 0 {
		log.Println("File not available")
		return
	}

	outContent := make([]byte, 0)

	// TODO: Could possible use a goroutine
	for _, version := range reply {
		for _, nID := range version.ReplicaList {
			_, ok := table[version.Filename]
			if ok {
				continue
			}
			table[version.Filename] = 1
			content := c.getFileFromNode(version.Filename, nID)
			fmt.Printf("Fetched %s version%d: \n----------------File begining--------------\n%s......%s\n------------------End File-----------------\n\n", filename, version.Version, content[:100], content[len(content)-100:])
			outContent = append(outContent, []byte(fmt.Sprintf("Version: %d: \n", version.Version))...)
			outContent = append(outContent, []byte("----------------File begining--------------\n\n")...)
			outContent = append(outContent, content...)
			outContent = append(outContent, []byte("\n------------------End File-----------------\n\n")...)
		}
	}

	c.writeFile("./fetched_files/"+outFileName, outContent)
}

func (c *Client) lsReplicasOfFile(filename string) {
	fmt.Printf("lsReplicasOfFile: filename: %s", filename)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		fmt.Printf("dialing: %s", err)
	}
	// reply: [nodeID1, nodeID2, ...]
	replicaList, err := c.callLsRPC(client, filename)
	if err != nil {
		fmt.Printf("lsReplicasOfFile: callLsRPC failed, err: %v", err)
	}

	fmt.Printf("File %s is replicated on nodes: \n", filename)
	for _, nodeID := range replicaList {
		fmt.Printf("\t%s", nodeID)
	}
	fmt.Println("")
}

func (c *Client) storesOnNode(nodeID string) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		fmt.Printf("dialing: %s", err)
	}

	// reply: [file1, file2, ...]
	fileList, err := c.callStoresRPC(client, nodeID)
	if err != nil {
		fmt.Printf("storesOnNode: callStoresRPC failed, err: %v", err)
	}

	fmt.Printf("Stores are: \n")
	for _, filename := range fileList {
		fmt.Printf("\t%s", filename)
	}
	fmt.Println("")
}

func (c *Client) memList() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		fmt.Printf("dialing: %s", err)
	}

	a := "a"
	b := "b"
	err = client.Call("SDFS.RPCPrintMemberList", &a, &b)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) index() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		fmt.Printf("dialing: %s", err)
	}

	a := "a"
	b := "b"
	err = client.Call("SDFS.RPCPrintIndex", &a, &b)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) rpcs() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.config.IP, c.config.Port))
	if err != nil {
		fmt.Printf("dialing: %s", err)
	}

	a := "a"
	b := "b"
	err = client.Call("SDFS.RPCPrintRPCClients", &a, &b)
	if err != nil {
		return err
	}
	return nil
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
	putFolder := flag.String("put-folder", "", "put-folder {folder}")
	deleteFilename := flag.String("del", "", "del {filename}")
	ls := flag.String("ls", "", "ls {filename}")
	stores := flag.String("stores", "", "stores {nodeID}")
	memList := flag.String("memList", "", "memList")
	index := flag.String("index", "", "index")
	rpcs := flag.String("rpcs", "", "rpcs")
	getVersions := flag.String("get-versions", "", "getVersions {sdfsfilename} {num-versions} {localfilenam}")
	// numVersions := flag.Int("numVersions", 0, "numVersion {number}")

	flag.Parse()

	if *getFilename != "" {
		c.getFile(*getFilename)
	} else if *putFilename != "" {
		c.putFile(*putFilename)
	} else if *putFolder != "" {
		c.putFolder(*putFolder)
	} else if *ls != "" {
		c.lsReplicasOfFile(*ls)
	} else if *stores != "" {
		c.storesOnNode(*stores)
	} else if *deleteFilename != "" {
		c.deleteFile(*deleteFilename)
	} else if *memList != "" {
		c.memList()
	} else if *index != "" {
		c.index()
	} else if *rpcs != "" {
		c.rpcs()
	} else if *getVersions != "" {
		args := os.Args[2:]
		if len(args) < 3 {
			fmt.Println("not enough args: getVersions {sdfsfilename} {num-versions} {localfilenam}")
		} else {
			t0 := time.Now()
			versions, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Printf("num-versions should be a number!")
			}
			//fmt.Printf("getVersions {%s} {%d} {%v}", args[0], versions, args[2])
			c.getVersionForFile(args[0], versions, args[2])
			fmt.Printf("Time for -get-versions with numVersions %d : %v\n", versions, time.Since(t0))
		}
	}

}
