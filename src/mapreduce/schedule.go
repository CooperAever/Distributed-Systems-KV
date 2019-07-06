package mapreduce

import (
	"fmt"
	"sync"
)
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// schedule() must give each worker a sequence of tasks
	// schedule() learns about the set of workers by reading its registerChan argument
	// schedule() tells a worker to execute a task by sending a worker.DoTask RPC to the worker
	// Use the call() function to send an RPC to a worker
	// 
	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	for i:=0;i<ntasks;i++{

		switch phase{
		case mapPhase:
			go Parallel(DoTaskArgs{jobName,mapFiles[i],phase,i,n_other},&wg,registerChan)
		case reducePhase:
			go Parallel(DoTaskArgs{jobName,"",phase,i,n_other},&wg,registerChan)
		}
		
	}

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
	return 
}

func Parallel(taskArgument DoTaskArgs,wg *sync.WaitGroup,registerChan chan string){
	address := <- registerChan
	if call(address,"Worker.DoTask",taskArgument,nil) == false {
		go Parallel(taskArgument,wg,registerChan)
		return 
	}
	go func () {registerChan<- address}()
	wg.Done()
}

// func mapParallel(taskArgument DoTaskArgs,wg *sync.WaitGroup,registerChan chan string){
// 	address := <- registerChan
// 	if call(address,"Worker.DoTask",taskArgument,nil) == false {
// 		go mapParallel(taskArgument,&wg,registerChan)
// 		break
// 	}
// 	go func () {registerChan<- address}()
// 	wg.Done()
// }

// func reduceParallel(taskArgument DoTaskArgs,wg *sync.WaitGroup,registerChan chan string){
// 	address := <- registerChan
// 	if call(address,"Worker.DoTask",taskArgument,nil) == false {
// 		go reduceParallel(taskArgument,&wg,registerChan)
// 		break
// 	}
// 	go func () {registerChan<- address}()
// 	wg.Done()
// }


// func receRegisterChan(add string,taskArgument DoTaskArgs,wg *sync.WaitGroup,registerChan chan string){
// 	call(add,"Worker.DoTask",taskArgument,nil)
// 	registerChan<- add
// 	wg.Done()
// }
