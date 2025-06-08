/*
mr-wc-all-final mr-wc-all-initial differ: byte 56, line 3
--- output changed after first worker exited
--- early exit test: FAIL

test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)

a process has exited. this means that the output should be finalized
otherwise, either a worker or the coordinator exited early


unexpected EOF
unexpected EOF
2023/09/01 10:37:09 not find givereduce
2023/09/01 10:37:09 not find givereduce
unexpected EOF
2023/09/01 10:37:09 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2023/09/01 10:37:09 not find givereduce
2023/09/01 10:37:09 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
2023/09/01 10:37:09 dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
cmp: EOF on mr-crash-all which is empty
--- crash output is not the same as mr-correct-crash.txt
--- crash test: FAIL
*** FAILED SOME TESTS
*/
package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}


type Coordinator struct {
	fileNames []string
	nReduce int
	allDone bool
	mapDone bool

	unreleasemap map[string]bool
	donemap []int

	unreleasereduce  map[int]bool
	donereduce []int
	CoorMutex sync.Mutex

	mapbeginsecond map[string]int64
	reducebeginsecond map[int]int64

}

type Mapreply struct {
	// worker passes this to the os package
	Filename string
	
	// marks a unique file for mapping
	// gives -1 for no more fileId
	Fileid int
	Nreduce int
}

type Reducereply struct {
	// worker passes this to the os package
	Filenames []string
	
	// marks a unique file for mapping
	// gives -1 for no more fileId
	
	Nreduceid int
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Isalldone(args *ExampleArgs, reply *bool) error {
	//reply.Y = args.X + 1
	if (len(c.donemap)==len(c.fileNames) && len(c.donereduce)==c.nReduce) {
		*reply=true
	}else{
		*reply=false
	}
	return nil
}


func (c *Coordinator) Givemap(args *ExampleArgs, reply *Mapreply) error {
	//reply.Y = args.X + 1
	c.CoorMutex.Lock()
	
	for k, v := range c.unreleasemap {
		if v{
			//reply.Fileid=1
			for i := 0; i < len(c.fileNames); i++ {
				if(c.fileNames[i]==k){
					reply.Fileid=i
				}
			}
			
			reply.Filename=k
			reply.Nreduce=c.nReduce
			c.unreleasemap[k]=false
			c.mapbeginsecond[k]=getNowTimeSecond()
			c.CoorMutex.Unlock()
			log.Println("coor give map:",reply.Fileid)
			return nil
		}
        
        
    }
	reply.Fileid=-1
	reply.Filename=""
	c.CoorMutex.Unlock()
	return nil
}
//Removemap
func (c *Coordinator) Removemap( reply *Mapreply, args *ExampleArgs) error {
//func (c *Coordinator) Removemap(args *ExampleArgs, reply *Mapreply2) error {
	c.CoorMutex.Lock()

	if (c.unreleasemap[reply.Filename]==false){
		c.donemap=append(c.donemap,reply.Fileid)
		log.Println("coor done map:",reply.Fileid,reply.Filename)
	}
	
	c.CoorMutex.Unlock()
	return nil
}

func (c *Coordinator) Givereduce(args *ExampleArgs, reply *Reducereply) error {
	//reply.Y = args.X + 1
	c.CoorMutex.Lock()
	/*
	for len(c.donemap)<len(c.fileNames){
		time.Sleep(10*time.Second)
	}
	*/
	if len(c.donemap)<len(c.fileNames){
		reply.Nreduceid=-1
		c.CoorMutex.Unlock()
		return nil
	}
	
	
	for k, v := range c.unreleasereduce {
		if v{
			reply.Nreduceid=k
			reply.Filenames=c.fileNames
			c.unreleasereduce[k]=false
			c.reducebeginsecond[k]=getNowTimeSecond()
			c.CoorMutex.Unlock()
			log.Println("coor give reduce:",reply.Nreduceid)
			return nil
		}
        
        
    }
	reply.Nreduceid=-1
	c.CoorMutex.Unlock()
	return nil
}

func (c *Coordinator) Removereduce( reply *Reducereply,args *ExampleArgs) error {
	c.CoorMutex.Lock()
	if (c.unreleasereduce[reply.Nreduceid]==false){
		c.donereduce=append(c.donereduce,reply.Nreduceid)
		log.Println("coor done reduce:",reply.Nreduceid)
	}
	
	c.CoorMutex.Unlock()
	return nil
}

//fileNames []string
//nReduce int
//mapbeginsecond map[string]int64
//reducebeginsecond map[int]int64
//unreleasemap map[string]bool
//donemap []int
//unreleasereduce  map[int]bool
//donereduce []int
func (c *Coordinator) removecrashtask(){
	for true {
		
		if c.Done(){
			break
		}
		
		time.Sleep(10*1000 * time.Millisecond)

		c.CoorMutex.Lock()
		log.Println("check crash")
		ret:=false
		for i := 0; i < len(c.fileNames); i++ {
			log.Println("map ",i,getNowTimeSecond() - c.mapbeginsecond[c.fileNames[i]])
			if (c.unreleasemap[c.fileNames[i]]==false && getNowTimeSecond() - c.mapbeginsecond[c.fileNames[i]]>10){
				ret = false
				for j:=0; j<len(c.donemap);j++{
					if c.donemap[j]==i{
						ret=true
					}
				}
				if ret==false{
					log.Println("map rollback",i)
					c.unreleasemap[c.fileNames[i]]=true
				}
				
			}
			
		}
		for i := 0; i < c.nReduce; i++ {
			if (c.unreleasereduce[i]==false && getNowTimeSecond() - c.reducebeginsecond[i]>10){


				ret = false
				for j:=0; j<len(c.donereduce);j++{
					if c.donereduce[j]==i{
						ret=true
					}
				}
				if ret==false{
					log.Println("reduce rollback",i)
					c.unreleasereduce[i]=true
				}
				
			}

			
		}
		c.CoorMutex.Unlock()
		
	}

}



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
	c.CoorMutex.Lock()
	ret := true

	if (len(c.donemap)==len(c.fileNames) && len(c.donereduce)==c.nReduce) {
		ret=true
	}else{
		ret=false
	}
	c.CoorMutex.Unlock()
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
	//log.Println("making coordinator")
	c.nReduce=nReduce

	// Your code here.
	c.unreleasemap=make(map[string]bool)
	for i := 0; i < len(files); i++ {
		//log.Printf("sending %vth file map task to channel\n", i)
		c.unreleasemap[files[i]]=true
	}
	c.unreleasereduce=make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		//log.Printf("sending %vth reduce task to channel\n", i)
		c.unreleasereduce[i]=true
	}
	c.mapbeginsecond=make(map[string]int64)
	c.reducebeginsecond=make(map[int]int64)
	c.fileNames=files
	c.mapDone=false
	//c.donemap= make([]int)
	/*
	for i := 0; i < 8; i++ {
		c.donemap=append(c.donemap,0)
	}
	*/
	c.server()

	go c.removecrashtask()
	return &c
}
