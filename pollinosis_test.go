package pollinosis

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

type CustomListener struct {}
func (c *CustomListener) NodeShuttingDown() {
	fmt.Println("NodeShuttingDown")
}
func (c *CustomListener) NodeUnloaded(replicaID, shardID uint64) {
	fmt.Println("NodeUnloaded", replicaID, shardID)
}
func (c *CustomListener) NodeDeleted(replicaID, shardID uint64) {
	fmt.Println("NodeDeleted", replicaID, shardID)
}
func (c *CustomListener) NodeReady(replicaID, shardID uint64) {
	fmt.Println("NodeReady", replicaID, shardID)
}
func (c *CustomListener) MembershipChanged(replicaID, shardID uint64) {
	fmt.Println("MembershipChanged", replicaID, shardID)
}
func (c *CustomListener) ConnectionEstablished(address string, snapshot bool) {
	fmt.Println("ConnectionEstablished", address, snapshot)
}
func (c *CustomListener) ConnectionFailed(address string, snapshot bool) {
	fmt.Println("ConnectionFailed", address, snapshot)
}
func (c *CustomListener) LogUpdated(log []byte, index uint64) {
	fmt.Println("LogUpdated", string(log), index)
}
func (c *CustomListener) LogRead(i interface{}) {
	fmt.Println("LogRead", i)
}
func (c *CustomListener) LeaderUpdated(leaderID, shardID, replicaID, term uint64) {
	fmt.Println("LeaderUpdated", leaderID, shardID, replicaID, term)
}

// TestGetSet 最小测试集(例子)
func TestGetSet(t *testing.T) {
	// 新建一个实例
	// replicaID - 节点ID
	// shardID - 集群ID
	// electionRTT - 选举的RTT(要大于heartbeatRTT, 接近整体节点的平均值, 假设3个节点electionRTT = 100, heartbeatRTT = 33)
	// heartbeatRTT - 同上解释
	// rttMillisecond - 集群内通信的延迟(毫秒)网络越好数字推荐越小
	// snapshotEntries - 快照自动备份的频率,默认关闭即可
	// compactionOverhead - 切割日志时最后保留多少条
	// bindAddress - 本地监听的IP和端口
	// dataDir - 数据持久化的目录
	// join - 是否为新增的节点
	p := New(1,
		100,
		10,
		2,
		200,
		0,
		1000,
		"127.0.0.1:10001",
		".",
		false,
		map[uint64]string{1:"127.0.0.1:10001"})

	if err := p.Start(); err != nil {
		log.Fatal(err)
	}

	log.Println("pollinosis started...")

	if leaderID, isLeader, err := p.Ready(time.Second * 5); err != nil {
		log.Fatal(err)
	}else{
		log.Println("pollinosis ready leaderID", leaderID, "isLeader", isLeader)
	}

	if err := p.Set(time.Second * 5, "K", "V"); err != nil {
		log.Fatal(err)
	}

	if value, err := p.Get(time.Second * 5, "K"); err != nil || value != "V" {
		log.Fatal(err)
	}else{
		log.Println("pollinosis get", "K", value)
	}

	p.Stop()

	_ = os.RemoveAll("1")
}
