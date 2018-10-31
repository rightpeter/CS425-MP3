package main

import (
	"CS425/CS425-MP3/model"
	"fmt"
	"reflect"
)

// Index Index struct
type Index struct {
	index model.GlobalIndexFile
	// map from node to num of files on the node
	numFiles map[string]int
}

// NewIndex creates a new index object
func NewIndex() *Index {
	var i Index
	i.index.Filename = make(map[string]model.FileStructure)
	i.index.Fileversions = make(map[string][]model.FileVersion)
	i.index.NodesToFile = make(map[string][]model.FileStructure)
	i.index.FileToNodes = make(map[string][]string)
	return &i
}

// AddNewNode AddNewNode
func (i *Index) AddNewNode(ip string) {
	i.numFiles[ip] = 0
}

// RemoveNode RemoveNode
func (i *Index) RemoveNode(ip string) []string {
	delete(i.numFiles, ip)
	// delete from global file index as well
	// Will also need to rereplicate and send new list of nodes with
	return nil
}

func (i *Index) removeFromSlice(ind int, slice []interface{}) []interface{} {
	if ind == len(slice) {
		return slice[:ind]
	}
	return append(slice[:ind], slice[ind+1:])
}
func (i *Index) findIndex(list []string, elem string) int {
	for ind, e := range list {
		if e == elem {
			return ind
		}
	}
	return -1
}

func (i *Index) getNodesWithLeastFiles() []string {
	return nil
}

// AddOrUpdateFile AddOrUpdateFile
func (i *Index) AddOrUpdateFile(filename, hash []byte) {

}

// AddFile add file for first time
func (i *Index) addFile(filename string, hash []byte) {
	nodes := i.getNodesWithLeastFiles()
	fs := model.FileStructure{
		Version:  0,
		Filename: filename,
		Hash:     hash,
	}
	i.index.Filename[filename] = fs
	for ind, ip := range nodes {
		i.numFiles[ip]++

		fv := model.FileVersion{
			Version: i.index.Filename[filename].Version,
			Nodes:   nodes,
			Hash:    hash,
		}

		i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)
		i.index.NodesToFile[ip] = append(i.index.NodesToFile[filename], fs)
		i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], ip)
	}

}

// UpdateFile update file
func (i *Index) updateFile(filename string, hash []byte) {
	if reflect.DeepEqual(i.index.Filename[filename].Hash, hash) {
		return
	}
	nodes := i.index.FileToNodes[filename]
	fs := model.FileStructure{
		Version:  i.index.Filename[filename].Version + 1,
		Filename: filename,
		Hash:     hash,
	}
	i.index.Filename[filename] = fs
	for ind, ip := range nodes {
		i.numFiles[ip]++

		fv := model.FileVersion{
			Version: i.index.Filename[filename].Version,
			Nodes:   nodes,
			Hash:    hash,
		}

		i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)
		i.index.NodesToFile[ip] = append(i.index.NodesToFile[filename], fs)
		i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], ip)
	}
}

// RemoveFile add file to GlobalIndexFile
func (i *Index) RemoveFile(filename string) {
	nodes := i.index.FileToNodes[filename]
	for ind, ip := range nodes {
		i.numFiles[ip]--
		delete(i.index.Fileversions, filename)
		var newFiles []model.FileStructure
		for ind, fs := range i.index.NodesToFile[ip] {
			if fs.Filename != filename {
				newFiles = append(newFiles, fs)
			}
		}
		i.index.NodesToFile[ip] = newFiles
		delete(i.index.FileToNodes, filename)
	}
}

// GetNodesWithFile get nodes
func (i *Index) GetNodesWithFile(filename string) []string {
	v, ok := i.index.FileToNodes[filename]
	if !ok {
		return nil
	}
	return v
}

// GetFilesOnNode get files
func (i *Index) GetFilesOnNode(IP string) []model.FileStructure {
	v, ok := i.index.NodesToFile[IP]
	if !ok {
		return nil
	}
	return v
}

func main() {
	i := NewIndex()
	i.AddFile("f1", "ip1")
	i.AddFile("f1", "ip2")
	i.AddFile("f1", "ip3")
	i.AddFile("f2", "ip2")
	i.AddFile("f2", "ip3")
	i.AddFile("f3", "ip1")
	i.AddFile("f1", "ip3")

	println("Files on ip1")
	fmt.Println(i.GetFilesOnNode("ip1"))
	println("Nodes with on f1")
	fmt.Println(i.GetNodesWithFile("f1"))

	fmt.Println("files: ", i.index.Files)
	fmt.Println("nodes: ", i.index.Nodes)
	fmt.Println("-----------------")
	fmt.Println("Removing f1")
	i.RemoveFile("f1")
	fmt.Println("files: ", i.index.Files)
	fmt.Println("nodes: ", i.index.Nodes)
	fmt.Println("Removing f2 and f3")
	i.RemoveFile("f2")
	i.RemoveFile("f3")
	fmt.Println("files: ", i.index.Files)
	fmt.Println("nodes: ", i.index.Nodes)
	fmt.Println("Removing f2 and f3 again")
	i.RemoveFile("f2")
	i.RemoveFile("f3")
	fmt.Println("files: ", i.index.Files)
	fmt.Println("nodes: ", i.index.Nodes)

	println("Files on ip1")
	fmt.Println(i.GetFilesOnNode("ip1"))
	println("Nodes with on f1")
	fmt.Println(i.GetNodesWithFile("f1"))

}
