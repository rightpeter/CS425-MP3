package main

import (
	"CS425/CS425-MP3/model"
	"fmt"
)

// Index Index struct
type Index struct {
	index model.GlobalIndexFile
}

// NewIndex creates a new index object
func NewIndex() *Index {
	var i Index
	i.index.Files = make(map[string][]string)
	i.index.Nodes = make(map[string][]string)
	return &i
}

// AddFile add file to GlobalIndexFile
func (i *Index) AddFile(filename string, IP string) {
	if i.findIndex(i.index.Files[filename], IP) == -1 {
		i.index.Files[filename] = append(i.index.Files[filename], IP)
	}
	if i.findIndex(i.index.Nodes[IP], filename) == -1 {
		i.index.Nodes[IP] = append(i.index.Nodes[IP], filename)
	}
}

func (i *Index) findIndex(list []string, elem string) int {
	for ind, e := range list {
		if e == elem {
			return ind
		}
	}
	return -1
}

// RemoveFile add file to GlobalIndexFile
func (i *Index) RemoveFile(filename string) {
	nodes, ok := i.index.Files[filename]
	if !ok {
		return
	}
	for _, ip := range nodes {
		delI := i.findIndex(i.index.Nodes[ip], filename)
		if delI+1 < len(i.index.Nodes[ip]) {
			i.index.Nodes[ip] = append(i.index.Nodes[ip][:delI], i.index.Nodes[ip][delI+1])
		} else {
			i.index.Nodes[ip] = i.index.Nodes[ip][:delI]
		}
	}
	delete(i.index.Files, filename)
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
}
