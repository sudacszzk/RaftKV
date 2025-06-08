
package mr


import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

// import "math/rand"
import "os"
import "sort"
import "io/ioutil"
import "time"
// import "time"
import "encoding/json"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

type MyWoker struct {

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	maporreduce bool
	
}

func keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}




func askmap (reply *Mapreply){
	args := ExampleArgs{}
	

	ok:=call("Coordinator.Givemap",&args, reply)
	if (ok){

		
	}else{
		log.Println("not find givemap")
		

	}
}

func removemap (reply *Mapreply){
	args := ExampleArgs{}
	

	ok:=call("Coordinator.Removemap",reply,&args )
	if (ok){

		
	}else{
		log.Println("not find removemap")
		

	}

}

func askreduce (reply *Reducereply){
	args := ExampleArgs{}
	

	ok:=call("Coordinator.Givereduce",&args, reply)
	if (ok){

		
	}else{
		log.Println("not find givereduce")
		

	}

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

func readIntermediates(filename string) []KeyValue {
	
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: ", filename)
	}else{
		//log.Println("success open file for read: ", filename)
	}
	
	dec := json.NewDecoder(file)
	
	
	kva := make([]KeyValue, 0)
	
	
	for {
		var kv KeyValue
		
		err = dec.Decode(&kv)
		
		if err != nil {
			break
		}
		kva = append(kva, kv)
		
	}
	
	
	file.Close()
	
	//log.Println("kva len：",len(kva))
	return kva
}


func exemap(reply *Mapreply,mapf func(string, string) []KeyValue)  {
	intermediate := []KeyValue{}
	filename:=reply.Filename
	//log.Println("opening file:",filename)
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
	//追加一个新的切片
	intermediate = append(intermediate, kva...)
	nReduce:=reply.Nreduce
	kvas := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		// ioutils deprecated
		// use this as equivalent
		//tempfile, err := os.CreateTemp(".", "mrtemp")
		tempfile, err := ioutil.TempFile(".", "mrtemp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		outname := fmt.Sprintf("mr-%v-%v", reply.Fileid, i)
		//outname := fmt.Sprintf("mr-%s-%v", filename, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			log.Println("fail write",outname)
		}else{
			log.Println("success write",outname,"len:",len(kvas[i]))
			//log.Println("worker map done",reply.Filename,reply.Fileid)

		}

	}
	
	args := ExampleArgs{}
	
	
	ok:=call("Coordinator.Removemap",&reply,&args);
	if ok{
		//log.Println("have removed maptask",reply2.Fileid,reply2.Filename)
		
	} 

	

}


func exereduce(reply *Reducereply,reducef func(string, []string) string) {
	
	//log.Println("executing reduce task",reply.Nreduceid)


	outname := fmt.Sprintf("mr-out-%v", reply.Nreduceid)
	// ofile, err := os.Open(outname)
	intermediate := make([]KeyValue, 0)
	//log.Println(reply.Filenames)
	//log.Println(len(reply.Filenames))
	for i := 0; i < len(reply.Filenames); i++ {
		//log.Println("generating intermediates on file:", i, len(reply.Filenames))
		
		readname := fmt.Sprintf("mr-%v-%v", i, reply.Nreduceid)
		intermediate = append(intermediate, readIntermediates(readname)...)
		//log.Println("success generated intermediates on file:", i, len(reply.Filenames))
	}
	//log.Println("total intermediate count ", len(intermediate))
	//tempfile, err := os.CreateTemp(".", "mrtemp")
	tempfile, err := ioutil.TempFile(".", "mrtemp")
	
	if err != nil {
		log.Println("cannot create tempfile for %v\n", outname)
	}
	//reduceKVSlice(intermediate, worker.reducef, tempfile)

	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		//
		
		//
		i = j
	}

	

	tempfile.Close()
	err = os.Rename(tempfile.Name(), outname)
	if err != nil {
		log.Println("rename tempfile failed for ", outname)
	}else{
		log.Println("success write",outname,"len:",len(intermediate))
	}
		

	args := ExampleArgs{}

	ok:=call("Coordinator.Removereduce", reply,&args);
	if ok{


	}else{
		
	}
	

}

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker:=MyWoker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.maporreduce=true //true for map false for reduce
	
	
	for true{
		//call("Coordinator.Example", &args, &reply)
		//log.Println("ask whether done")
		args := ExampleArgs{}
		reply :=false
		ok:=call("Coordinator.Isalldone",&args, &reply);
		if ok{
			if reply{
				log.Println("all done worker")
				break
			}
			
		} 
		
		map_reply :=Mapreply{}
		//map_reply2 :=Mapreply2{}
		askmap(&map_reply)
		if (map_reply.Fileid>=0){
			log.Println("worker give map ",map_reply.Filename,map_reply.Fileid)
			exemap(&map_reply,worker.mapf);
			
			
			continue
			
		}else{
			//log.Println("map task have released")
			//break
		}

		reduce_reply :=Reducereply{}
		askreduce(&reduce_reply)
		if (reduce_reply.Nreduceid>=0){
			log.Println("worker give reduce ",reduce_reply.Nreduceid)
			//log.Println("reduce task:",reduce_reply.Nreduceid)
			exereduce(&reduce_reply,worker.reducef);
			continue
			
		}else{
			time.Sleep(time.Second)

			//log.Println("reduce task have released")
			//break
		}

		
		


	}
	
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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