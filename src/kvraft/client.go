/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-04-09 10:32:59
 */
package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex int // 上次请求成功的leader

	// 问题来了 如何使clientId唯一
	clientId  int64 // 对应的clientId 用于标记是哪个client
	requestId int64 // Incremental 对应当前client请求序号 client+requestId用于表示一条op记录

	mu sync.Mutex // Lock
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
	// You'll have to add code here.

	// 生成clientId
	ck.leaderIndex = 0 // 默认从0开始遍历
	ck.clientId = nrand() % 10000
	ck.requestId = 0 // 从1开始 进入函数就递增
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("<Client-%d> <Get> (%s)", ck.clientId, key)
	ck.requestId++
	i := ck.leaderIndex
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderIndex = i
				DPrintf("=Client-%03d= [GET] server[%d] return (%s => %s)", ck.clientId, ck.leaderIndex, key, "")
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				ck.leaderIndex = i
				return ""
			}
		}
		DPrintf("<Client-%03d> [GET] server[%d] not valid leader", ck.clientId, i)
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("<Client-%d> <%s> (%s => %s)", ck.clientId, op, key, value)
	// You will have to modify this function.
	ck.requestId++
	i := ck.leaderIndex
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderIndex = i
				DPrintf("=Client-%03d= [%s] server[%d] return (%s => %s)", ck.clientId, op, ck.leaderIndex, key, value)
				return
			}
		}
		DPrintf("<Client-%03d> [%s] server[%d] not valid leader", ck.clientId, args.Op, i)
		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
