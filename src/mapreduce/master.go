package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  map_chan := make([] chan bool, mr.nMap)
  for i := 0; i < mr.nMap; i++ {
      map_chan[i] = make(chan bool)
  }
  map_idx_chan := make(chan bool)
  for i:= 0; i < mr.nMap; {
      go func() {
          var arg DoJobArgs
          var res DoJobReply
          arg.File = mr.file
          arg.Operation = Map
          arg.JobNumber = i
          map_idx_chan <- true
          arg.NumOtherPhase = mr.nReduce
          <- mr.idleWorkersListChannel
          f := mr.IdleWorkersList.Front()
          mr.idleWorkersLock.Lock()
          mr.IdleWorkersList.Remove(f)
          mr.idleWorkersLock.Unlock()
          worker_addr := f.Value.(string)
          call(worker_addr, "Worker.DoJob", &arg, &res)
          mr.idleWorkersLock.Lock()
          mr.IdleWorkersList.PushBack(worker_addr)
          mr.idleWorkersLock.Unlock()
          mr.idleWorkersListChannel <- true
          map_chan[arg.JobNumber] <- true
      }()
      <- map_idx_chan
      i++
  }
  for i, ch := range map_chan {
      fmt.Println("map chan range: ", i, "\n")
      <- ch
  }
  fmt.Println("map complete\n")
  reduce_chan := make([] chan bool, mr.nReduce)
  for i := 0; i < mr.nReduce; i++ {
      reduce_chan[i] = make(chan bool)
  }
  reduce_idx_chan := make(chan bool)
  for i:= 0; i < mr.nReduce; {
      go func() {
          var arg DoJobArgs
          var res DoJobReply
          arg.File = mr.file
          arg.Operation = Reduce 
          arg.JobNumber = i
          reduce_idx_chan <- true
          arg.NumOtherPhase = mr.nMap
          <- mr.idleWorkersListChannel
          f := mr.IdleWorkersList.Front()
          mr.idleWorkersLock.Lock()
          mr.IdleWorkersList.Remove(f)
          mr.idleWorkersLock.Unlock()
          worker_addr := f.Value.(string)
          call(worker_addr, "Worker.DoJob", &arg, &res)
          mr.idleWorkersLock.Lock()
          mr.IdleWorkersList.PushBack(worker_addr)
          mr.idleWorkersLock.Unlock()
          mr.idleWorkersListChannel <- true
          reduce_chan[arg.JobNumber] <- true
      }()
      <- reduce_idx_chan
      i++
  }
  for i, ch := range reduce_chan {
      fmt.Println("reduce chan range: ", i, "\n")
      <- ch
  }
  return mr.KillWorkers()
}
