package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	reply := CallExample2()
	fmt.Printf("reply.Y %v\n", reply)
	filename := reply.Y
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	save := make([]map[string][]string, reply.reduce)
	for i := 0; i < len(save); i++ {
		save[i] = make(map[string][]string)
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		hashkey := ihash(kva[i].Key) % reply.reduce
		save[hashkey][kva[i].Key] = values

		// this is the correct format for each line of Reduce output.

		i = j
	}
	for i := 0; i < len(save); i++ {
		file := "mr-" + reply.num + "-" + strconv.Itoa(i)
		asfile, _ := os.Create(file)
		enc := json.NewEncoder(asfile)
		enc.Encode(&save[i])
	}

	// save:=make([][]KeyValue, reply.NReduce)
	// for _,key:= range kva {
	// 	if (key.Value !="1"){
	// 		fmt.Println("error: %v",key)
	// 	}

	// }
	// asfile, _ := os.Create("json.txt")
	// enc := json.NewEncoder(asfile)
	// enc.Encode(kva)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample2() ExampleReply2 {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply2{"0","1",1}
	fmt.Printf("reply.Y %v\n", reply.reduce)
	fmt.Printf("reply.Y %v\n", reply.Y)
	fmt.Printf("reply.Y %v\n", reply.num)
	// send the RPC request, wait for the reply.
	call("Master.Example2", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply)
	reply.reduce = 10
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply)
	return reply
}

// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	fmt.Printf("reply.Y %v\n", reply)
	err = c.Call(rpcname, args, reply)
	fmt.Printf("replyis.Y %v\n", reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
