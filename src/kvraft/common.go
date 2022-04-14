/*
 * @Author: lxk
 * @Date: 2022-02-22 21:50:25
 * @LastEditors: lxk
 * @LastEditTime: 2022-04-08 15:40:13
 */
package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}
