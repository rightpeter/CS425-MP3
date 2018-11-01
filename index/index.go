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
func (i *Index) AddNewNode(id string) {
	i.numFiles[id] = 0
}

// RemoveNode RemoveNode
func (i *Index) RemoveNode(id string) []model.PullInstruction {
	instructions := []model.PullInstruction{}
	delete(i.numFiles, id)
	// delete from global file index as well
	nodes := i.getNodesWithLeastFiles()
	filesOnNode := i.index.NodesToFile[id]
	delete(i.index.NodesToFile, id)

	for _, file := range filesOnNode {
		// remove file from FileToNodes
		ind := i.findIndex(i.index.FileToNodes[file.Filename], id)
		// log.Panicf("Removed %s, %")
		if ind != -1 {
			i.index.FileToNodes[file.Filename] = i.removeFromSlice(ind, i.index.FileToNodes[file.Filename])
		}
		for _, node := range nodes {
			// send only the latest file version for replication
			if i.index.Filename[file.Filename].Hash == file.Hash && !i.nodeHasFile(file.Filename, node) {
				// send file and break
				inst := model.PullInstruction{
					Filename: fmt.Sprintf("%s_%d", file.Filename, file.Version),
					Node:     node,
					PullFrom: i.GetNodesWithFile(file.Filename), // some node which has filen
				}
				instructions = append(instructions, inst)

				// update NodeToFile and FileToNodes
				fs := model.FileStructure{
					Version:  file.Version,
					Filename: file.Filename,
					Hash:     file.Hash,
				}
				i.index.NodesToFile[node] = append(i.index.NodesToFile[node], fs)

				// update FileToNodes
				i.index.FileToNodes[file.Filename] = append(i.index.FileToNodes[file.Filename], node)

				//TODO update the Nodes in Fileversions

				break
			}
		}
	}

	return instructions

	// Will also need to rereplicate and send new list of nodes with

	// get list of nodes sorted by num of files on them
	// for files on removed node:
	// if node does not comtain file, give it an instruction to go pull file from a node with that file
	// Will also need to change Nodes to File and FileToNodes accordingly
	return nil
}

func (i *Index) removeFromSlice(ind int, slice []string) []string {
	log.Println("removeFromSlice ", slice, ind)
	if ind == len(slice) {
		return slice[:ind]
	}
	return append(slice[:ind], slice[ind+1:]...)
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
	lenid := make(map[int][]string)
	var lens []int
	for k, v := range i.numFiles {
		_, ok := lenid[v]
		if !ok {
			lenid[v] = make([]string, 0)
		}
		lenid[v] = append(lenid[v], k)
		if len(lens) > 0 && lens[len(lens)-1] != v {
			lens = append(lens, v)
		} else if len(lens) == 0 {
			lens = append(lens, v)
		}
	}
	log.Println("getNodesWithLeastFiles lens:", lens)
	sort.Ints(lens)
	var Sortedids []string
	for _, val := range lens {
		Sortedids = append(Sortedids, lenid[val]...)
	}
	return Sortedids
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

func (i *Index) nodeHasFile(filename, id string) bool {
	for _, val := range i.index.NodesToFile[id] {
		if val.Filename == filename {
			return true
		}
	}
	return false
}

// AddFile add file for first time
func (i *Index) addFile(filename string, hash [SIZE]byte) {
	nodes := i.getNodesWithLeastFiles()
	replicas := REPLICAS

	log.Println("Nodes with least files: ", nodes)
	nodesWithFile := make([]string, 0)

	fs := model.FileStructure{
		Version:  0,
		Filename: filename,
		Hash:     hash,
	}
	i.index.Filename[filename] = fs
	fv := model.FileVersion{
		Version: i.index.Filename[filename].Version,
		Hash:    hash,
	}

	for _, id := range nodes {
		if i.nodeHasFile(filename, id) {
			continue
		}
		if replicas <= 0 {
			break
		}
		replicas--
		i.numFiles[id]++

		// Get old file version or create new and append nodes
		nodesWithFile = append(nodesWithFile, id)
		for _, f := range i.index.Fileversions[filename] {
			if f.Version == i.index.Filename[filename].Version {
				fv = f
			}
		}
		fv.Nodes = append(fv.Nodes, id)
		i.index.NodesToFile[id] = append(i.index.NodesToFile[id], fs)
		i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], id)
	}
	fv.Nodes = nodesWithFile
	i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)

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

	fv := model.FileVersion{
		Version: i.index.Filename[filename].Version,
		Hash:    hash,
	}
	nodesWithFile := make([]string, 0)
	for _, id := range nodes {
		i.numFiles[id]++

		for _, f := range i.index.Fileversions[filename] {
			if f.Version == i.index.Filename[filename].Version {
				fv = f
			}
		}
		fv.Nodes = append(fv.Nodes, id)

		i.index.NodesToFile[id] = append(i.index.NodesToFile[id], fs)
		nodesWithFile = append(nodesWithFile, id)
		if !i.nodeHasFile(filename, id) {
			i.index.FileToNodes[filename] = append(i.index.FileToNodes[filename], id)
		}
	}
	fv.Nodes = nodesWithFile
	i.index.Fileversions[filename] = append(i.index.Fileversions[filename], fv)
}

// RemoveFile add file to GlobalIndexFile
func (i *Index) RemoveFile(filename string) {
	nodes := i.index.FileToNodes[filename]
	for _, id := range nodes {
		i.numFiles[id]--
		delete(i.index.Fileversions, filename)
		var newFiles []model.FileStructure
		for _, fs := range i.index.NodesToFile[id] {
			if fs.Filename != filename {
				newFiles = append(newFiles, fs)
			}
		}
		i.index.NodesToFile[id] = newFiles
		delete(i.index.FileToNodes, filename)
	}
}

func (i *Index) GetVersions(filename string, numVersions int) []model.FileVersion {
	versions := i.index.Fileversions[filename]
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version > versions[j].Version
	})
	log.Println(versions)
	if numVersions > len(versions) {
		return versions
	}
	return versions[:numVersions]
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
func (i *Index) GetFilesOnNode(id string) []model.FileStructure {
	v, ok := i.index.NodesToFile[id]
	if !ok {
		return nil
	}
	return v
}

func (i *Index) GetFile(filename string) (int, []string) {
	versions := i.index.Fileversions[filename]
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version > versions[j].Version
	})
	return versions[0].Version, versions[0].Nodes
}

// GetGlobalIndexFile return GlobalIndexFile
func (i *Index) GetGlobalIndexFile() model.GlobalIndexFile {
	return i.index
}

func main() {
	i := NewIndex()
	i.AddNewNode("id1")
	i.AddNewNode("id2")
	i.AddNewNode("id3")
	i.AddNewNode("id4")
	i.AddNewNode("id5")
	i.AddNewNode("id6")

	i.AddFile("f1", md5.Sum([]byte("f1")))
	i.AddFile("f2", md5.Sum([]byte("f2")))
	i.AddFile("f2", md5.Sum([]byte("f2a")))
	i.AddFile("f3", md5.Sum([]byte("f3")))
	i.AddFile("f3", md5.Sum([]byte("f3a")))

	println("Files on id1")
	fmt.Println(i.GetFilesOnNode("id1"))
	println("Files on id2")
	fmt.Println(i.GetFilesOnNode("id2"))
	println("Nodes with f1")
	fmt.Println(i.GetNodesWithFile("f1"))
	println("Nodes with f2")
	fmt.Println(i.GetNodesWithFile("f2"))
	println("Nodes with f3")
	fmt.Println(i.GetNodesWithFile("f3"))

	// fmt.Println("----- Removing f2 -----")
	// i.RemoveFile("f2")
	// println("Nodes with f1")
	// fmt.Println(i.GetNodesWithFile("f1"))
	// println("Nodes with f2")
	// fmt.Println(i.GetNodesWithFile("f2"))
	// println("Nodes with f3")
	// fmt.Println(i.GetNodesWithFile("f3"))

	fmt.Println("----- Removing id1 -----")
	println("Files on id1")
	fmt.Println(i.GetFilesOnNode("id1"))
	inst := i.RemoveNode("id1")
	fmt.Println("inst: ", inst)
	println("Files on id1")
	fmt.Println(i.GetFilesOnNode("id1"))
	println("Nodes with f1")
	fmt.Println(i.GetNodesWithFile("f1"))
	println("Nodes with f2")
	fmt.Println(i.GetNodesWithFile("f2"))
	println("Nodes with f3")
	fmt.Println(i.GetNodesWithFile("f3"))

	fmt.Println("----------")
	fmt.Println("Get file versions")
	fmt.Println(i.GetVersions("f3", 5))
	fmt.Println("----------")
	fmt.Println("Getfile ")
	fmt.Println(i.GetFile("f3"))
}
