package index

import (
	"CS425/CS425-MP3/model"
	"crypto/md5"
	"fmt"
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
func NewIndex() Index {
	var i Index
	i.index.Filename = make(map[string]model.FileStructure)
	i.index.Fileversions = make(map[string][]model.FileVersion)
	i.index.NodesToFile = make(map[string][]model.FileStructure)
	i.index.FileToNodes = make(map[string][]string)
	i.numFiles = make(map[string]int)
	return i
}

// LoadFromGlobalIndexFile crLoadFromGlobalIndexFile
func LoadFromGlobalIndexFile(file model.GlobalIndexFile) Index {
	var i Index
	i.index = model.GlobalIndexFile{
		Filename:     file.Filename,
		Fileversions: file.Fileversions,
		NodesToFile:  file.NodesToFile,
		FileToNodes:  file.FileToNodes,
	}
	i.numFiles = make(map[string]int)
	return i
}

// AddNewNode AddNewNode
func (i *Index) AddNewNode(id string) {
	// log.Printf("Index: Added new node %v", id)
	i.numFiles[id] = 0
}

// PrintIndex PrintIndex
func (i *Index) PrintIndex() string {
	ret := ""
	ret += "Nodes to files:\n"
	for k, v := range i.index.NodesToFile {
		ret += fmt.Sprintf("%s: %v\n", k, v)
	}

	ret += "\nFile to Nodes:\n"
	for k, v := range i.index.FileToNodes {
		ret += fmt.Sprintf("%s: %v\n", k, v)
	}
	return ret
}

// LsReplicasOfFile ls replicas of file
func (i *Index) LsReplicasOfFile(filename string) []string {
	return i.index.FileToNodes[filename]
}

// StoresOnNode return files stored on node
func (i *Index) StoresOnNode(nodeID string) []string {
	files := []string{}
	for _, file := range i.index.NodesToFile[nodeID] {
		files = append(files, file.Filename)
	}
	return files
}

func (i *Index) getLatestVersion(filename string) int {
	return i.index.Filename[filename].Version
}

func (i *Index) getLatestFileVersion(filename string) model.FileVersion {
	for _, fv := range i.index.Fileversions[filename] {
		if fv.Version == i.getLatestVersion(filename) {
			return fv
		}
	}
	return model.FileVersion{}
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
		ind := i.findIndex(i.index.FileToNodes[file.Filename], id)
		for _, fv := range i.index.Fileversions[file.Filename] {
			ind := i.findIndex(fv.Nodes, id)
			if ind != -1 {
				fv.Nodes = i.removeFromSlice(ind, fv.Nodes)
			}
		}

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

				//update Fileversions
				latestFV := i.getLatestFileVersion(file.Filename)
				latestFV.Nodes = append(latestFV.Nodes, node)

				break
			}
		}
	}

	return instructions
}

func (i *Index) removeFromSlice(ind int, slice []string) []string {
	// log.Println("removeFromSlice ", slice, ind)
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
	// log.Println("getNodesWithLeastFiles lens:", lens)
	sort.Ints(lens)
	var Sortedids []string
	for _, val := range lens {
		Sortedids = append(Sortedids, lenid[val]...)
	}
	return Sortedids
}

// AddFile AddFile
func (i *Index) AddFile(filename string, hash [SIZE]byte) (int, []string) {
	_, ok := i.index.Filename[filename]
	if !ok {
		// log.Println("Adding new file: ", filename)
		return i.addFile(filename, hash)
	}
	// log.Println("Updating file: ", filename)
	return i.updateFile(filename, hash)
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
func (i *Index) addFile(filename string, hash [SIZE]byte) (int, []string) {
	nodes := i.getNodesWithLeastFiles()
	replicas := REPLICAS

	// log.Println("Nodes with least files: ", nodes)
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

	return i.index.Filename[filename].Version, nodesWithFile
}

// UpdateFile update file
func (i *Index) updateFile(filename string, hash [SIZE]byte) (int, []string) {
	if reflect.DeepEqual(i.index.Filename[filename].Hash, hash) {
		return i.index.Filename[filename].Version, i.index.FileToNodes[filename]
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
	return i.index.Filename[filename].Version, nodesWithFile
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
	// log.Println(versions)
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
	if len(versions) == 0 {
		return -1, nil
	}
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
