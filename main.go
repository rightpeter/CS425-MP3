package main

import (
	"CS425/CS425-MP3/sdfs"
	"flag"
	"io/ioutil"
	"log"
	"os"
)

// This function will register and initiate server
func main() {
	// parse argument
	configFilePath := flag.String("c", "./config.json", "Config file path")
	fdConfigFilePath := flag.String("fdc", "./failure_detector.config.json", "Failure Detector Config file path")

	// load config file
	configFile, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	// load fd config file
	fdConfigFile, err := ioutil.ReadFile(*fdConfigFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	// Class for server
	s := sdfs.NewSDFS(configFile, fdConfigFile)

	f, err := os.OpenFile(s.GetConfigPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	s.StartFailureDetector()
}
