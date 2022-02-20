package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by hash key.
type ByHashKey []KeyValue

func (a ByHashKey) Len() int           { return len(a) }
func (a ByHashKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHashKey) Less(i, j int) bool { return ihash(a[i].Key) < ihash(a[j].Key) }

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	req := TaskArgs{
		TaskId:   -1,
		Finished: false,
		TaskType: TASK_TYPE_IDLE,
	}
	for {

		reply := TaskReply{}
		if !call("Coordinator.TaskCenterHandle", &req, &reply) {
			break
		}
		switch reply.TaskType {
		case TASK_TYPE_MAP:
			//不考虑路径问题
			fileList := task_map(&reply, mapf)
			req = TaskArgs{
				TaskId:   reply.TaskId,
				TaskType: TASK_TYPE_MAP,
				Finished: true,
				MapFile:  fileList,
			}
			continue
		case TASK_TYPE_REDUCE:
			task_reduce(&reply, reducef)
			req = TaskArgs{
				TaskId:   reply.TaskId,
				TaskType: TASK_TYPE_REDUCE,
				Finished: true,
			}
			continue
		default:
			req = TaskArgs{
				TaskId:   -1,
				TaskType: TASK_TYPE_IDLE,
				Finished: false,
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	log.Println("worker quit")
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

func task_map(reply *TaskReply, mapf func(string, string) []KeyValue) []string {
	// defer recoverf()

	mapfile, err := os.Open(reply.Filename)
	if err != nil {
		// panic(errors.New(fmt.Sprintf("cannot open %v", reply.Filename)))
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(mapfile)
	if err != nil {
		// panic(errors.New(fmt.Sprintf("cannot read %v", reply.Filename)))
		log.Fatalf("cannot read %v", reply.Filename)
	}
	mapfile.Close()
	intermediate := mapf(reply.Filename, string(content))

	//临时目录
	tmpPath := os.TempDir() + "/" + strconv.FormatInt(time.Now().Unix(), 10)
	os.Mkdir(tmpPath, os.ModePerm)
	//创建中间文件

	ofile := make([]*os.File, reply.NReduce)
	for i := 0; i < len(ofile); i++ {
		defer ofile[i].Close()
	}

	tmpFileName := make([]string, 0)
	for i := 0; i < len(ofile); i++ {
		//intermediate filename mr-xy
		//x: map task id
		//y reduce bucket
		filename := "mr-" + strconv.Itoa(reply.TaskId) + strconv.Itoa(i)
		ofile[i], err = os.Create(tmpPath + "/" + filename)
		if err != nil {
			//崩溃处理
			// panic(errors.New(fmt.Sprintf("can not create %v", ofile[i])))
			log.Fatalf("can not create %v", ofile[i])
		}
		tmpFileName = append(tmpFileName, ofile[i].Name())
	}

	//将key按nReduce分散到各个bucket文件中
	for _, kv := range intermediate {
		n := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder(ofile[n])
		err := enc.Encode(&kv)
		if err != nil {
			// panic(errors.New(fmt.Sprintf("can not write %v", ofile[n])))
			log.Fatalf("can not write %v", ofile[n])
		}
	}

	return tmpFileName
}

func recoverf() {
	err := recover()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		fmt.Println("buf", string(buf))
	}

}

func task_reduce(reply *TaskReply, reducef func(string, []string) string) {
	// defer recoverf()

	//读取所有该nReduce的中间文件
	fileList := reply.MapFile
	// fmt.Printf("reduce func fileList %v\n",fileList)
	kvs := make([]KeyValue, 0)
	for i := 0; i < len(fileList); i++ {
		file, err := os.Open(fileList[i])
		defer file.Close()
		if err != nil {
			// panic(errors.New(fmt.Sprintf("can not open file may be interminate file had delete by other program\n")))
			log.Fatalf("can not open file may be interminate file had delete by other program\n")
		}
		jd := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			err = jd.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	//排序
	sort.Sort(ByKey(kvs))

	filename := "mr-out-" + strconv.Itoa(reply.TaskId)
	// tmpPath := os.TempDir() + "/" + strconv.FormatInt(time.Now().Unix(), 10)
	//删除文件夹所有内容
	// defer os.RemoveAll(tmpPath)
	// os.Mkdir(tmpPath, os.ModePerm)
	os.Remove(filename)
	// _, err := os.Stat(filename)
	// if err == nil {
	// 	panic(errors.New(fmt.Sprintf("file had create by other program\n")))
	// }

	// ofile, err := os.Create(tmpPath + "/" + filename)
	//检查是否一有人

	ofile, err := os.Create(filename)

	if err != nil {
		// panic(errors.New(fmt.Sprintf("create file err\n")))
		log.Fatalf("create file err\n")
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	// os.Rename(ofile.Name(), filename)
}
