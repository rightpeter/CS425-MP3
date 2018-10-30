package main

import "CS425/CS425-MP3/model"

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
	i.index.Files[filename] = append(i.index.Files[filename], IP)
	i.index.Nodes[IP] = append(i.index.Nodes[IP], filename)
}

// // RemoveFile add file to GlobalIndexFile
// func (i *Index) RemoveFile(filename string, IP string) {
// 	nodes, ok := i.index.Files[filename]
// 	// File doesn't exist
// 	if !ok {
// 		return
// 	}
// 	for ind, ip := range nodes {
// 		// remove filename form nodes list

// 	}
// }

func main() {

}
