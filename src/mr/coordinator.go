package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mut     sync.Mutex
	nReduce int
	//下一个应分配的map task id
	nextMapId int
	//下一个应分配的reduce task id
	nextReduceId int
	//已完成的map数量
	nFinishedMapTask int
	//已完成的reduce数量
	nFinishedReduceTask int
	files               []string
	//任务状态map reduce阶段用,0为未完成,1为完成
	mapTaskStatus    map[int]int
	reduceTaskStatus map[int]int
	//保存map处理后的中间文件路径,key为reduceid value为
	mapFile map[int][]string
	done    bool
}

// Your code here -- RPC handlers for the worker to call.

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

	// Your code here.
	c.mut.Lock()
	ret = c.done
	c.mut.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.files = files
	c.mapFile = make(map[int][]string)
	c.done = false
	c.nFinishedMapTask = 0
	c.nFinishedReduceTask = 0
	c.nextMapId = 0
	c.nextReduceId = 0
	c.mapTaskStatus = make(map[int]int)
	c.reduceTaskStatus = make(map[int]int)
	for i := 0; i < len(files); i++ {
		c.mapTaskStatus[i] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = 0
	}
	fmt.Printf("input files %v\n", len(c.files))

	c.server()
	return &c
}

func (c *Coordinator) TaskCenterHandle(req *TaskArgs, reply *TaskReply) error {
	//finished task
	switch req.TaskType {
	case TASK_TYPE_IDLE:
		//分配新任务
		c.mut.Lock()
		defer c.mut.Unlock()

		distributeTask(c, req, reply)
	case TASK_TYPE_MAP:
		c.mut.Lock()
		defer c.mut.Unlock()
		if req.Finished {
			if c.mapTaskStatus[req.TaskId] != 1 {
				fmt.Printf("map finished id %v\n",req.TaskId)
				c.nFinishedMapTask++
				c.mapTaskStatus[req.TaskId] = 1
				//将各文件路径按nReduce编码插入
				for i := 0; i < c.nReduce; i++ {
					v, ok := c.mapFile[i]
					if !ok {
						v = make([]string, 0)
					}
					v = append(v, req.MapFile[i])
					c.mapFile[i] = v
				}
			}
		}

		distributeTask(c, req, reply)
	case TASK_TYPE_REDUCE:
		c.mut.Lock()
		defer c.mut.Unlock()

		if req.Finished {
			if c.reduceTaskStatus[req.TaskId] != 1 {
				fmt.Printf("reduce finished id %v\n",req.TaskId)
				c.reduceTaskStatus[req.TaskId] = 1
				c.nFinishedReduceTask++
			}
			if c.nFinishedReduceTask >= c.nReduce {
				c.done = true
				return nil
			}

		}

		distributeTask(c, req, reply)
	}

	return nil
}

func distributeTask(c *Coordinator, req *TaskArgs, reply *TaskReply) {
	//分配map任务
	if c.nFinishedMapTask < len(c.files) {
		//未分配完情况
		if c.nextMapId < len(c.files) {
			reply.Filename = c.files[c.nextMapId]
			reply.NReduce = c.nReduce
			reply.TaskId = c.nextMapId
			reply.TaskType = TASK_TYPE_MAP
			fmt.Printf("distribute map task id %v\n", c.nextMapId)
			c.nextMapId++
		} else {
			//分配完但有部分map没有及时完成
			//查找未完成部分
			time.Sleep(time.Millisecond * 500)
			for i := 0; i < len(c.mapTaskStatus); i++ {
				if c.mapTaskStatus[i] == 0 {
					reply.Filename = c.files[i]
					reply.NReduce = c.nReduce
					reply.TaskId = i
					reply.TaskType = TASK_TYPE_MAP
					fmt.Printf("distribute unfinished map task id %v\n", i)
					break
				}
			}
		}
	} else if c.nFinishedReduceTask < c.nReduce {
		//分配reduce任务
		if c.nextReduceId < c.nReduce {
			reply.MapFile = c.mapFile[c.nextReduceId]
			reply.NReduce = c.nReduce
			reply.TaskId = c.nextReduceId
			reply.TaskType = TASK_TYPE_REDUCE
			fmt.Printf("distribute reduce task id %v\n", c.nextReduceId)
			c.nextReduceId++
		} else {
			//分配完但有部分reduce没有及时完成
			//查找未完成部分
			time.Sleep(time.Millisecond * 500)
			for i := 0; i < len(c.reduceTaskStatus); i++ {
				if c.reduceTaskStatus[i] == 0 {
					reply.MapFile = c.mapFile[i]
					reply.NReduce = c.nReduce
					reply.TaskId = i
					reply.TaskType = TASK_TYPE_REDUCE
					fmt.Printf("distribute unfinished reduce task id %v\n", i)
					break
				}
			}
		}
	} else {
		//所有任务都完成
	}
}
