package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Master struct {
	all_files []string
	used      int
	usedfiles []string
	reduce    int

	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Example2(args *ExampleArgs, reply *ExampleReply2) error {
	if m.used < len(m.all_files) {
		
		reply.num = strconv.Itoa(m.used)
		reply.Y = m.all_files[m.used]
		reply.reduce = m.reduce
		fmt.Printf("masterreduceis%v", reply)
		m.used += 1

	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.all_files = files
	m.used = 0
	m.reduce = nReduce
	fmt.Printf("reduceis%v", m.reduce)
	// Your code here.

	m.server()
	return &m
}
