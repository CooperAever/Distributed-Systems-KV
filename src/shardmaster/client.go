package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
// import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	// used for detect duplicate
	seq int 
	lastLeader int 
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	// will initialize seq and lastLeader to 0 by default
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// args := &QueryArgs{}
	// Your code here.
	ck.seq ++
	// i := ck.lastLeader
	args := QueryArgs{
		Num : num,
		ClientId : ck.clientId,
		Seq : ck.seq,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// args := &JoinArgs{}
	// Your code here.
	// args.Servers = servers
	ck.seq ++
	args := JoinArgs{
		Servers : servers,
		ClientId : ck.clientId,
		Seq : ck.seq,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// args := &LeaveArgs{}
	// Your code here.
	// args.GIDs = gids
	ck.seq ++
	args := LeaveArgs{
		GIDs : gids,
		ClientId : ck.clientId,
		Seq : ck.seq,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// args := &MoveArgs{}
	// Your code here.
	// args.Shard = shard
	// args.GID = gid
	ck.seq ++
	args := MoveArgs{
		Shard : shard,
		GID : gid,
		ClientId : ck.clientId,
		Seq : ck.seq,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
