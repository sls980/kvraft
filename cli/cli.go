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

type CmdFunc func(cli *rpc.Client, args []string) (err error)

const (
	TimeoutInterval     = 3000 * time.Millisecond
	RETRY_INTERNAL      = 100 * time.Millisecond
	DEFAULT_RETRY_TIMES = 3
)

var CmdMaps map[string]CmdFunc = map[string]CmdFunc{"PUT": Put, "GET": Get, "DEL": Del, "HELP": Help, "EXIT": Exit}

var ClusterNodes = struct {
	idx   int
	nodes []string
}{
	idx:   0,
	nodes: make([]string, 0, 3),
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <ip:port> [ip:port] ...\n", os.Args[0])
		return
	}

	// store cluster node
	for _, node := range os.Args[1:] {
		ClusterNodes.nodes = append(ClusterNodes.nodes, node)
	}

	fmt.Println("Welcome to kvraft client.")

	input := bufio.NewReader(os.Stdin)
	for {
		// show last connect node
		fmt.Printf("[%d]> ", ClusterNodes.idx)
		buf, err := input.ReadString('\n')
		if err != nil {
			log.Fatal("input error", err)
		}

		buf = strings.Trim(buf, "\n")
		cmds := strings.Split(buf, " ")

		if len(cmds) < 1 {
			_ = Help(nil, nil)
			continue
		}

		if fun, ok := CmdMaps[strings.ToUpper(cmds[0])]; ok {
			if err := Retry(len(ClusterNodes.nodes), func() (e error) {
				cli, e := DailServer()
				if e != nil {
					return
				}
				return fun(cli, cmds[1:])
			}); err != nil {
				fmt.Printf("Exec failed, err=%v\n", err)
				continue
			}
		} else {
			_ = Help(nil, nil)
		}
	}
}

func Retry(times int, f func() error) (err error) {
	if times < 1 {
		times = DEFAULT_RETRY_TIMES
	}

	for i := 0; i < times; i++ {
		if err = f(); err != nil {
			time.Sleep(RETRY_INTERNAL)
		} else {
			return
		}
	}

	return
}

func DailServer() (*rpc.Client, error) {
	cli, err := rpc.DialHTTP("tcp", ClusterNodes.nodes[ClusterNodes.idx])
	// switch to next node
	ClusterNodes.idx++
	if ClusterNodes.idx >= len(ClusterNodes.nodes) {
		ClusterNodes.idx = 0
	}

	return cli, err
}

func Get(cli *rpc.Client, args []string) (err error) {
	if len(args) != 1 {
		return Help(nil, nil)
	}

	getArgs := raft.GetArgs{[]byte(args[0])}
	getReply := raft.GetReply{}

	err = cli.Call("Raft.Get", getArgs, &getReply)
	if err != nil || !getReply.Ok {
		return fmt.Errorf("Raft.Get fail: %v", err)
	}

	fmt.Println(string(getReply.Value))
	return
}

func Put(cli *rpc.Client, args []string) (err error) {
	if len(args) != 2 {
		return Help(nil, nil)
	}

	putArgs := raft.PutArgs{Key: []byte(args[0]), Value: []byte(args[1]), TimeoutInterval: int(TimeoutInterval)}
	putReply := raft.PutReply{}

	err = cli.Call("Raft.Put", putArgs, &putReply)
	if err != nil || !putReply.Ok {
		return fmt.Errorf("Raft.Put fail: %v", err)
	}

	fmt.Println("Done")
	return
}

func Del(cli *rpc.Client, args []string) (err error) {
	if len(args) != 1 {
		return Help(nil, nil)
	}

	delArgs := raft.DelArgs{Key: []byte(args[0]), TimeoutInterval: int(TimeoutInterval)}
	delReply := raft.DelReply{}

	err = cli.Call("Raft.Del", delArgs, &delReply)
	if err != nil || !delReply.Ok {
		return fmt.Errorf("Raft.Del fail: %v", err)
	}

	fmt.Println("Done")
	return
}

func Help(cli *rpc.Client, args []string) (err error) {
	fmt.Println("HELP: HELP show usage")
	fmt.Println("PUT:  PUT <key> <value>")
	fmt.Println("GET:  GET <key>")
	fmt.Println("DEL:  DEL <key>")
	fmt.Println("EXIT: EXIT quit program")
	return
}

func Exit(cli *rpc.Client, args []string) (err error) {
	os.Exit(0)
	return
}
