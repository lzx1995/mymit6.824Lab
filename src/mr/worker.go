package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
import "encoding/json"
import "time"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type ByKeys []KeyValueArr

func (a ByKeys) Len() int           { return len(a) }
func (a ByKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeys) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueArr  struct {
	Key string
	Values []string
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


func renameFilename(ofiles [](*os.File), filename []string) {
	for idx, ofile := range ofiles {
		os.Rename(ofile.Name(), filename[idx])
	}
}

func saveMapFile(mapTaskNum int, nReduce int, intermediate []KeyValue) {


	ofiles := [](*os.File){}
	onames := []string{}

	for i := 0; i < nReduce;  i++ {
		oname := fmt.Sprintf("mr-%d-%d", mapTaskNum, i)
		onames  = append(onames, oname)
		ofile, _ := ioutil.TempFile("", oname)
		ofiles = append(ofiles, ofile)
	}

	for i := 0; i < len(intermediate);  {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		kv := KeyValueArr{intermediate[i].Key, values}
		if(intermediate[i].Key == "A") {
			//fmt.Printf("key: A len: %v\n", len(values))
		}
		//fmt.Printf("key: %s hash: %d\n", intermediate[i].Key,ihash(intermediate[i].Key) % nReduce)
		enc := json.NewEncoder(ofiles[ihash(intermediate[i].Key) % nReduce])
		enc.Encode(&kv)
		i = j
	}

	renameFilename(ofiles, onames)
}

func Reduce(filenames []string, reduceTaskNum int, reducef func(string, []string) string) {
	intermediate := []KeyValueArr{}
	//fmt.Println(filenames)
	for _, filename := range filenames {
		
		file, err :=  os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
  		for {
    		var kv KeyValueArr
    		if err := dec.Decode(&kv); err != nil {
      			break
    		}

			
    		intermediate = append(intermediate, kv)
			
  		}
		
		file.Close()

	}

	sort.Sort(ByKeys(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceTaskNum)
	ofile, _ := ioutil.TempFile("", oname)
	
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Values...)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	 //CallExample()
	 pid := os.Getpid()

	 intermediate := []KeyValue{}
	 //fmt.Println(pid)
	 args :=RpcArgs{}
	 args.MapTaskNum = -1
	 args.ReduceTaskNum = -1
	 args.WorkerId = pid

	 for {
		reply := RpcReply{}

		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			if reply.Status == -1 {
				args = RpcArgs{}
				args.WorkerId = pid
				args.MapTaskNum = -1
	 			args.ReduceTaskNum = -1
				time.Sleep(10 * time.Millisecond )
			} else if reply.Status == 0 {	// 任务已经全部完成
				//fmt.Printf("All Finished")
				break
				//log.Fatalf("All Finished")
			} else if reply.Status == 1 {	// 分配了Map任务
				//fmt.Println(reply.Filename)
				file, err := os.Open(reply.Filename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				//fmt.Println(string(content))
				file.Close()
				kva := mapf(reply.Filename, string(content))

				intermediate = append(intermediate, kva...)
				sort.Sort(ByKey(intermediate))
				saveMapFile(reply.MapTaskNum, reply.ReduceCount, intermediate)
				intermediate = []KeyValue{}
				//fmt.Println(reply.MapTaskNum)
				args.MapTaskNum = reply.MapTaskNum
				args.Status = 1
				//args.ReduceTaskNum = -1
				// worker identif//y
				//pid := os.Getpid()
				//fmt.Println(pid)
			} else {	// 分配了Reduce任务
				//log.Fatalf("Reducing")
				//fmt.Printf("PID:%d\n", pid)
				Reduce(reply.ReduceFilenames, reply.ReduceTaskNum, reducef)
				//args.MapTaskNum = -1
				args.Status = 2
				args.ReduceTaskNum = reply.ReduceTaskNum
				//fmt.Println(reply.ReduceFilenames)
			}

		} else {
			log.Fatalf("GetTask Fail")
		}
		//fmt.Printf("reply Status:%v PID:%d\n", reply.Status, pid)
		//time.Sleep(1 * time.Second)
		
	 }

	 //fmt.Printf("alen:%d\n", alen)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
