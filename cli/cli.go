package main

import (
	"bufio"
	"fmt"
	"kvraft/raft"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type CmdFunc func(cli *rpc.Client, args []string)

const (
	TimeoutInterval = 3000 * time.Millisecond
)

var CmdMaps map[string]CmdFunc = map[string]CmdFunc{"PUT": Put, "GET": Get, "DEL": Del, "HELP": Help, "EXIT": Exit}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: ./cli <ip:port>")
		return
	}

	cli, err := rpc.DialHTTP("tcp", os.Args[1])
	if err != nil {
		log.Fatal("Dail error", err)
	}

	defer cli.Close()

	fmt.Println("Welcome to kvraft client.")

	input := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		buf, err := input.ReadString('\n')
		if err != nil {
			log.Fatal("input error", err)
		}

		buf = strings.Trim(buf, "\n")
		cmds := strings.Split(buf, " ")

		if len(cmds) < 1 {
			Help(nil, nil)
			continue
		}

		if fun, ok := CmdMaps[strings.ToUpper(cmds[0])]; ok {
			fun(cli, cmds[1:])
		} else {
			Help(nil, nil)
		}
	}
}

func Get(cli *rpc.Client, args []string) {
	if len(args) != 1 {
		Help(nil, nil)
		return
	}

	getArgs := raft.GetArgs{[]byte(args[0])}
	getReply := raft.GetReply{}

	err := cli.Call("Raft.Get", getArgs, &getReply)
	if err != nil || !getReply.Ok {
		fmt.Println("Raft.Get", err)
		return
	}

	fmt.Println(string(getReply.Value))
}

func Put(cli *rpc.Client, args []string) {
	if len(args) != 2 {
		Help(nil, nil)
		return
	}

	putArgs := raft.PutArgs{Key: []byte(args[0]), Value: []byte(args[1]), TimeoutInterval: int(TimeoutInterval)}
	putReply := raft.PutReply{}

	err := cli.Call("Raft.Put", putArgs, &putReply)
	if err != nil || !putReply.Ok {
		fmt.Println("Raft.Put", err)
		return
	}

	fmt.Println("Done")
}

func Del(cli *rpc.Client, args []string) {
	if len(args) != 1 {
		Help(nil, nil)
		return
	}

	delArgs := raft.DelArgs{Key: []byte(args[0]), TimeoutInterval: int(TimeoutInterval)}
	delReply := raft.DelReply{}

	err := cli.Call("Raft.Del", delArgs, &delReply)
	if err != nil || !delReply.Ok {
		fmt.Println("Raft.Del", err)
		return
	}

	fmt.Println("Done")
}

func Help(cli *rpc.Client, args []string) {
	fmt.Println("HELP: HELP show usage")
	fmt.Println("PUT:  PUT <key> <value>")
	fmt.Println("GET:  GET <key>")
	fmt.Println("DEL:  DEL <key>")
	fmt.Println("EXIT: EXIT quit program")
}

func Exit(cli *rpc.Client, args []string) {
	os.Exit(0)
}
