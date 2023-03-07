package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

import "fmt"


type Coordinator struct {
	// Your definitions here.
	// MapReduce任务数
	NReduceTask int
	NMapTask int

	// 已经完成的任务个数
	MappedCount int
	ReducedCount int

	// 每个任务的完成状态 yes:完成 no：未完成
	NMap []bool
	NReduce []bool

	// 该任务正在执行
	Mapping []bool
	Reducing []bool

	// map状态：false：未完成 yes：完成
	MapStatus bool
	// reduce状态：false：未完成 yes：完成
	ReduceStatus bool
	mu sync.Mutex

	Filenames []string

}

// Map倒计时
func Timming(c *Coordinator, mapTaskNum int) {
	time.Sleep(10 * time.Second)
	//fmt.Println("Timming")
	c.mu.Lock()
	defer c.mu.Unlock()
	if(c.NMap[mapTaskNum] == false) {
		c.Mapping[mapTaskNum] = false

		//log.Fatalf("Fail Map Task Number %v", mapTaskNum)
	}
}

// Reduce倒计时
func ReduceTimming(c *Coordinator, reduceTaskNum int) {
	time.Sleep(10 * time.Second)
	//fmt.Println("Timming")
	c.mu.Lock()
	defer c.mu.Unlock()
	if(c.NReduce[reduceTaskNum] == false) {
		c.Reducing[reduceTaskNum] = false
		//log.Fatalf("Fail Reduce Task Number %v", reduceTaskNum)
	}
}


func (c *Coordinator) SendMapTask(args *RpcArgs, reply *RpcReply) {
	// 对已经完成的Map任务计数

	count := 0
	for index, val := range c.NMap {
		if val == false && c.Mapping[index] == false {	// 未分配的Map任务
			c.Mapping[index] = true
			reply.Status = 1
			reply.Filename = c.Filenames[index]
			reply.ReduceCount = c.NReduceTask
			reply.MapTaskNum = index
			//fmt.Printf("PID:%d\n", args.WorkerId)
			go Timming(c, index)
			break
		} else if val == true{
			count++
		}
		if(count == c.NMapTask) {	// Map任务已经完成
			c.MapStatus = true
			reply.Status = 2
		}
	}
}

func (c *Coordinator) SendReduceTask(args *RpcArgs, reply *RpcReply) {
	count := 0
	for  index, val := range c.NReduce {
		if val == false && c.Reducing[index] == false {	// 未分配的Reduce任务
			c.Reducing[index] = true
			reply.Status = 2
			filenames := []string{}
			for i := 0; i < c.NMapTask; i++ {
				//fmt.Println(args.ReduceTaskNum)
				filename := fmt.Sprintf("mr-%d-%d", i, index)
				filenames = append(filenames, filename)
			}
			reply.Status = 2
			reply.ReduceCount = c.NReduceTask
			reply.ReduceTaskNum = index
			reply.ReduceFilenames = filenames
			//fmt.Printf("PID:%d\n", args.WorkerId)
			go ReduceTimming(c, index)
			break
		} else if val == true {
			count++
		}
		
		if count == c.NReduceTask {
			c.ReduceStatus = true
			
			reply.Status = 0
		}
		//fmt.Printf("PID:%d count: %d reply Status:%v\n", args.WorkerId, count, reply.Status)
	}
}



// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask( args *RpcArgs, reply *RpcReply) error {
//func (c *Coordinator) GetTask(args *RpcArgs, reply *RpcReply) error {
	reply.Status = -1
	c.mu.Lock()
	defer c.mu.Unlock()
	if(c.MapStatus == true && c.ReduceStatus == true) {
		reply.Status = 0
	}
	if args.Status == 1 {	// 已经完成的Map任务
		//fmt.Printf("Map Success Num:%d\n", args.MapTaskNum)
		c.NMap[args.MapTaskNum] = true
		c.MappedCount++
	}
	if args.Status == 2 {	// 已经完成的Reduce任务
		c.NReduce[args.ReduceTaskNum] = true
		c.ReducedCount++
	}

	if c.MapStatus == false{
		c.SendMapTask(args, reply)
	}
	//fmt.Printf("PID:%d c.MapStatus:%v c.ReduceStatus:%v\n", args.WorkerId, c.MapStatus, c.ReduceStatus)
	if c.MapStatus == true && c.ReduceStatus == false {
		//fmt.Printf("PID:%d\n", args.WorkerId)
		c.SendReduceTask(args, reply)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.ReduceStatus
	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduceTask = nReduce
	c.NMapTask = len(files)
	//fmt.Println(files)
	// Your code here.
	for i := 0;  i < len(files); i++ {
		c.NMap = append(c.NMap, false)
		c.Mapping = append(c.Mapping, false)
		c.Filenames = append(c.Filenames, files[i])
	}
	for i := 0; i < nReduce; i++ {
		c.NReduce = append(c.NReduce, false)
		c.Reducing = append(c.Reducing, false)
	}
	//fmt.Println(len(c.NMap))
	c.server()
	return &c
}
