package main

import (
	"flag"
	"fmt"
	"kvraft/raft"
	"time"
)

func main() {
	//parse config file
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Println("Usage: server config.json")
		return
	}

	raft.MakeRaftServer(flag.Arg(0))

	for {
		time.Sleep(10e9)
	}
}
