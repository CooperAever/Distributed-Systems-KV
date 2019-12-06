package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"
import "math"
// import "fmt"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// 4A
	chMap map[int] chan Op
	cid2seq map[int64]int
	killCh chan bool
}


type Op struct {
	// Your data here.
	Cmd string // could be join/leave/move/query
	Args interface{}	// cmd's args
	ClientId int64
	Seq int
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// fmt.Println("handle Join now !!! ")
	originOp := Op{"Join",*args,args.ClientId,args.Seq}
	reply.WrongLeader = sm.templateHandler(originOp)

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// fmt.Println("handle Leave now !!! ")
	originOp := Op{"Leave",*args,args.ClientId,args.Seq}
	reply.WrongLeader = sm.templateHandler(originOp)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// fmt.Println("handle Move now !!! ")
	originOp := Op{"Move",*args,args.ClientId,args.Seq}
	reply.WrongLeader = sm.templateHandler(originOp)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// fmt.Println("handle query now !!! ")
	originOp := Op{"Query",*args,args.ClientId,args.Seq}
	reply.WrongLeader = sm.templateHandler(originOp)
	if !reply.WrongLeader{
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >= 0 && args.Num < len(sm.configs){
			reply.Config = sm.configs[args.Num]
		}else{
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}
}

func (sm *ShardMaster) templateHandler(originOp Op) bool{
	wrongLeader := true
	index,_,isLeader := sm.rf.Start(originOp)
	if !isLeader{ return wrongLeader}
	ch := sm.getCh(index,true)
	op := sm.beNotified(ch,index)
	if equalOp(op,originOp){
		wrongLeader = false
	}
	return wrongLeader
}

func (sm *ShardMaster) getCh(index int,createIfNotExists bool) chan Op{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _,ok := sm.chMap[index]; !ok{
		if !createIfNotExists{return nil}
		sm.chMap[index] = make(chan Op,1)
	}

	return sm.chMap[index]
}

func (sm *ShardMaster) beNotified(ch chan Op,index int) Op{
	select{
	case notifyArg := <- ch:
		close(ch)
		sm.mu.Lock()
		delete(sm.chMap,index)
		sm.mu.Unlock()
		return notifyArg
	case <- time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

func equalOp(a,b Op) bool{
	return a.Seq == b.Seq && a.ClientId == b.ClientId && a.Cmd == b.Cmd
}




//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// JOIN 会给一组GID -> SERVER的映射。其实就是把这些GID 组，加到MASTER的管理范围里来。那么有新的GROUP来了。每台机器可以匀一些SHARD过去
// LEAVE 是给一组GID，表示这组GID的SERVER机器们要走。那么他们管的SHARD又要匀给还没走的GROUP
// MOVE 是指定某个SHARD 归这个GID管
// QUERY就是根据CONFIG NUM来找到对应的CONFIG里的SHARD 规则是如何


func (sm *ShardMaster) updateConfig(op string,arg interface{}){
	// join and leave need to do balance
	cfg := sm.createNextConfig()
	if op == "Query"{

	}else if op == "Move"{
		moveArg := arg.(MoveArgs)
		if _,exists := cfg.Groups[moveArg.GID];exists{
			cfg.Shards[moveArg.Shard] = moveArg.GID
		}else{
			return
		}
	}else if op == "Join"{
		joinArg := arg.(JoinArgs)
		for gid,servers := range joinArg.Servers{
			newServers := make([]string,len(servers))
			copy(newServers,servers)
			cfg.Groups[gid] = newServers
			sm.rebalance(&cfg,op,gid)
		}
	}else if op=="Leave"{
		leaveArg := arg.(LeaveArgs)
		for _,gid := range leaveArg.GIDs{
			delete(cfg.Groups,gid)
			sm.rebalance(&cfg,op,gid)
		}
	}else{
		log.Fatal("invalid area",op)
	}
	sm.configs = append(sm.configs,cfg)
}


func (sm *ShardMaster) createNextConfig() Config{
	lastCfg := sm.configs[len(sm.configs)-1]
	nextCfg := Config{Num:lastCfg.Num +1,Shards:lastCfg.Shards,Groups:make(map[int][]string)}
	for gid,servers := range lastCfg.Groups{
		nextCfg.Groups[gid] = append([]string{},servers...)
	}
	return nextCfg
}




// 如果是LEAVE的话找最小的，把LEAVE的给最小的。随后再找最小的，一直到LEAVE的GROUP的SHARD没有了
func (sm *ShardMaster) rebalance(cfg *Config,request string,gid int){
	shardsCount := sm.groupByGid(cfg) 	//	gid -> shards
	switch request{
	// 算出之前CONFIG，每个GID 有几个SHARD。然后JOIN的话找最大的，移到新加进来的那个的。直到达到平均值（向下取整）
	case "Join":
		avg := NShards / len(cfg.Groups)
		for i:= 0;i<avg;i++{
			maxGid := sm.getMaxShardGid(shardsCount)
			cfg.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	// 如果是LEAVE的话找最小的，把LEAVE的给最小的。随后再找最小的，一直到LEAVE的GROUP的SHARD没有了
	case "Leave":
		shardsArray,exists := shardsCount[gid]
		if !exists {return}
		delete(shardsCount,gid)
		if len(cfg.Groups) == 0{	// remove all gid
			cfg.Shards = [NShards]int{}
			return
		}
		for _,v := range shardsArray{
			minGid := sm.getMinShardGid(shardsCount)
			cfg.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid],v)
		}
	}

}

// 建立group -> shards index的映射
func (sm *ShardMaster) groupByGid(cfg *Config) map[int][]int{
	shardsCount := map[int][]int{}

	for k,_ := range cfg.Groups{
		shardsCount[k] = []int{}
	}
	for k,v := range cfg.Shards{
		shardsCount[v] = append(shardsCount[v],k)
	}
	return shardsCount
}

func (sm *ShardMaster) getMaxShardGid(shardsCount map[int][]int) int{
	max := -1
	var gid int
	for k,v := range shardsCount{
		if max < len(v){
			max = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) getMinShardGid(shardsCount map[int][]int) int{
	min := math.MaxInt32
	var gid int
	for k,v := range shardsCount{
		if min > len(v){
			min = len(v)
			gid = k
		}
	}
	return gid
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	//4A
	labgob.Register(JoinArgs{})
    labgob.Register(LeaveArgs{})
    labgob.Register(MoveArgs{})
    labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	// 4A
	sm.chMap = make(map[int]chan Op)
	sm.cid2seq = make(map[int64] int)
	sm.killCh = make(chan bool,1)

	go func(){
		for{
			select{
			case <-sm.killCh:
				return
			case applyMsg := <- sm.applyCh:
				if !applyMsg.CommandValid{continue}
				op := applyMsg.Command.(Op)
				sm.mu.Lock()
				maxSeq,found := sm.cid2seq[op.ClientId]
				if op.Seq >=0 && (!found || op.Seq > maxSeq){
					sm.updateConfig(op.Cmd,op.Args)
					sm.cid2seq[op.ClientId] = op.Seq
				}
				sm.mu.Unlock()
				if notifyCh := sm.getCh(applyMsg.CommandIndex,false);notifyCh != nil{
					notifyCh <- op
				}
			}
		}
	}()

	return sm
}
