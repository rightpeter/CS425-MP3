package main

import (
	"CS425/CS425-MP3/index"
	"crypto/md5"
	"fmt"
	"time"
)

func main() {
	i := index.NewIndex()
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

	t0 := time.Now()
	fmt.Println("----------")
	fmt.Println("Get file versions")
	fmt.Println(i.GetVersions("f3", 5))
	fmt.Println("----------")
	fmt.Println("Getfile ")
	fmt.Println(i.GetFile("f7"))

	fmt.Printf("Time: %v\n", time.Since(t0))
}
