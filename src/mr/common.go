/*
 * @Author: lxk
 * @Date: 2022-06-02 20:28:33
 * @LastEditors: lxk
 * @LastEditTime: 2022-06-02 21:56:01
 */

package mr

// worker有两种角色
type RoleType int

const (
	Idle    RoleType = 0
	Mapper  RoleType = 1
	Reducer RoleType = 2
	Done    RoleType = 3 // Master通知Worker任务结束，退出
)

type TaskInfo struct {
	Role      RoleType // Map 还是reduce
	PartIndex int      // Mapper和Reducer的partition index 用于文件命名
	FileName  string   // 处理的文件名(map使用)
	ReduceNum int      // Reducer数量(map使用)
	MapNum    int      // Mapper数目(reduce使用)需要知道读取数目文件 本来应该是文件位置
}
