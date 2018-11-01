package main

import (
	"CS425/CS425-MP3/model"
	"crypto/md5"
	"fmt"
	"log"
	"reflect"
	"sort"
)

// SIZE md5 size
const SIZE = model.SIZE

// REPLICAS num of file repicas
const REPLICAS = 4

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
	i.numFiles = make(map[string]int)
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

func (i *Index) getNodesWithLeastFiles(table map[string]int) []string {
	log.Println("getNodesWithLeastFiles table: ", table)
	lenIP := make(map[int][]string)
	var lens []int
	for k, v := range table {
		_, ok := lenIP[v]
		if !ok {
			lenIP[v] = make([]string, 0)
		}
		lenIP[v] = append(lenIP[v], k)
		if len(lens) > 0 && lens[len(lens)-1] != v {
			lens = append(lens, v)
		} else if len(lens) == 0 {
			lens = append(lens, v)
		}
	}
	log.Println("getNodesWithLeastFiles lens:", lens)
	sort.Ints(lens)
	var SortedIPs []string
	for _, val := range lens {
		SortedIPs = append(SortedIPs, lenIP[val]...)
	}
	return SortedIPs
}

// AddFile AddFile
func (i *Index) AddFile(filename string, hash [SIZE]byte) {
	_, ok := i.index.Filename[filename]
	if !ok {
		log.Println("Adding new file: ", filename)
		i.addFile(filename, hash)
		return
	}
	log.Println("Updating file: ", filename)
	i.updateFile(filename, hash)
}

func (i *Index) nodeHasFile(filename, ip string) bool {
	for _, val := range i.index.NodesToFile[ip] {
		if val.Filename == filename {
			return true
		}
	}
	return false
}

// AddFile add file for first time
func (i *Index) addFile(filename string, hash [SIZE]byte) {
	replicas := REPLICAS
	nodes := i.getNodesWithLeastFiles(i.numFiles)
	log.Println("Nodes with least files: ", nodes)

	fs := model.FileStructure{
		Version:  0,
		Filename: filename,
		Hash:     hash,
	}

	i.index.Filename[filename] = fs

	for _, ip := range nodes {
		if i.nodeHasFile(filename, ip) {
			continue
		}
		if replicas <= 0 {
			break
		}
		replicas--
		i.numFiles[ip]++

		fv := model.FileVersion{
			Version: i.index.Filename[filename].Version,
			Nodes:   nodes, // incorrect, but might not use it
			Hash:    hash,
		}

		i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)
		i.index.NodesToFile[ip] = append(i.index.NodesToFile[ip], fs)
		i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], ip)
	}

}

// UpdateFile update file
func (i *Index) updateFile(filename string, hash [SIZE]byte) {
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
	for _, ip := range nodes {
		i.numFiles[ip]++

		fv := model.FileVersion{
			Version: i.index.Filename[filename].Version,
			Nodes:   nodes,
			Hash:    hash,
		}

		i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)
		i.index.NodesToFile[ip] = append(i.index.NodesToFile[ip], fs)
		if !i.nodeHasFile(filename, ip) {
			i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], ip)
		}
	}
}

// RemoveFile add file to GlobalIndexFile
func (i *Index) RemoveFile(filename string) {
	nodes := i.index.FileToNodes[filename]
	for _, ip := range nodes {
		i.numFiles[ip]--
		delete(i.index.Fileversions, filename)
		var newFiles []model.FileStructure
		for _, fs := range i.index.NodesToFile[ip] {
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
	i.AddNewNode("ip1")
	i.AddNewNode("ip2")
	i.AddNewNode("ip3")
	i.AddNewNode("ip4")
	i.AddNewNode("ip5")
	i.AddNewNode("ip6")

	i.AddFile("f1", md5.Sum([]byte("f1")))
	i.AddFile("f2", md5.Sum([]byte("f2")))
	i.AddFile("f2", md5.Sum([]byte("f2a")))
	i.AddFile("f3", md5.Sum([]byte("f3")))

	println("Files on ip1")
	fmt.Println(i.GetFilesOnNode("ip1"))
	println("Files on ip2")
	fmt.Println(i.GetFilesOnNode("ip2"))
	println("Nodes with f1")
	fmt.Println(i.GetNodesWithFile("f1"))
	println("Nodes with f2")
	fmt.Println(i.GetNodesWithFile("f2"))
	println("Nodes with f3")
	fmt.Println(i.GetNodesWithFile("f3"))

	fmt.Println("----- Removing f2 -----")
	i.RemoveFile("f2")
	println("Nodes with f1")
	fmt.Println(i.GetNodesWithFile("f1"))
	println("Nodes with f2")
	fmt.Println(i.GetNodesWithFile("f2"))
	println("Nodes with f3")
	fmt.Println(i.GetNodesWithFile("f3"))
	// fmt.Println("files: ", i.index.Files)
	// fmt.Println("nodes: ", i.index.Nodes)
	// fmt.Println("-----------------")
	// fmt.Println("Removing f1")
	// i.RemoveFile("f1")
	// fmt.Println("files: ", i.index.Files)
	// fmt.Println("nodes: ", i.index.Nodes)
	// fmt.Println("Removing f2 and f3")
	// i.RemoveFile("f2")
	// i.RemoveFile("f3")
	// fmt.Println("files: ", i.index.Files)
	// fmt.Println("nodes: ", i.index.Nodes)
	// fmt.Println("Removing f2 and f3 again")
	// i.RemoveFile("f2")
	// i.RemoveFile("f3")
	// fmt.Println("files: ", i.index.Files)
	// fmt.Println("nodes: ", i.index.Nodes)

	// println("Files on ip1")
	// fmt.Println(i.GetFilesOnNode("ip1"))
	// println("Nodes with on f1")
	// fmt.Println(i.GetNodesWithFile("f1"))

}
