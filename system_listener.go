package pollinosis

import (
	"github.com/lni/dragonboat/v4/raftio"
)

type defaultEventListener struct{}

var defaultEvent = &defaultEventListener{}

// LogUpdated 日志已更新事件
func (rl *defaultEventListener) LogUpdated(key, value string, index uint64) {
	//fmt.Println("LogUpdated", "Key:", key, "Value:", value, "Index:", index)
}

// LogRead 日志读取事件
func (rl *defaultEventListener) LogRead(key string) {
	//fmt.Println("LogRead", "Key:", key)
}

// LeaderUpdated Leader被更新事件
func (rl *defaultEventListener) LeaderUpdated(leaderID, shardID, replicaID, term uint64) {
	//fmt.Println("LeaderUpdated", "LeaderID:", leaderID, "ShardID:", shardID, "ReplicaID:", replicaID, "Term:", term)
}

// NodeShuttingDown 节点正在关闭事件
func (rl *defaultEventListener) NodeShuttingDown() {
	//fmt.Println("NodeShuttingDown")
}

// NodeUnloaded 节点被卸载事件
func (rl *defaultEventListener) NodeUnloaded(replicaID, shardID uint64) {
	//fmt.Println("NodeUnloaded", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// NodeDeleted 节点被删除事件
func (rl *defaultEventListener) NodeDeleted(replicaID, shardID uint64) {
	//fmt.Println("NodeDeleted", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// NodeReady 节点准备完成事件
func (rl *defaultEventListener) NodeReady(replicaID, shardID uint64) {
	//fmt.Println("NodeReady", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// MembershipChanged 集群内成员发生变动事件
func (rl *defaultEventListener) MembershipChanged(replicaID, shardID uint64) {
	//fmt.Println("MembershipChanged", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// ConnectionEstablished 网络连接已建立事件
func (rl *defaultEventListener) ConnectionEstablished(address string, isSnapshot bool) {
	//fmt.Println("ConnectionEstablished", "Address:", address, "IsSnapshot:", isSnapshot)
}

// ConnectionFailed 网络连接失败事件
func (rl *defaultEventListener) ConnectionFailed(address string, isSnapshot bool) {
	//fmt.Println("ConnectionFailed", "Address:", address, "IsSnapshot:", isSnapshot)
}

func (p *Pollinosis) LeaderUpdated(info raftio.LeaderInfo) {
	for _, event := range p.event {
		event.LeaderUpdated(info.LeaderID, info.ShardID, info.ReplicaID, info.Term)
	}
}

func (p *Pollinosis) NodeHostShuttingDown() {
	for _, event := range p.event {
		event.NodeShuttingDown()
	}
}

func (p *Pollinosis) NodeUnloaded(info raftio.NodeInfo) {
	for _, event := range p.event {
		event.NodeUnloaded(info.ReplicaID, info.ShardID)
	}
}

func (p *Pollinosis) NodeDeleted(info raftio.NodeInfo) {
	for _, event := range p.event {
		event.NodeDeleted(info.ReplicaID, info.ShardID)
	}
}

func (p *Pollinosis) NodeReady(info raftio.NodeInfo) {
	for _, event := range p.event {
		event.NodeReady(info.ReplicaID, info.ShardID)
	}
}

func (p *Pollinosis) MembershipChanged(info raftio.NodeInfo) {
	for _, event := range p.event {
		event.MembershipChanged(info.ReplicaID, info.ShardID)
	}
}

func (p *Pollinosis) ConnectionEstablished(info raftio.ConnectionInfo) {
	for _, event := range p.event {
		event.ConnectionEstablished(info.Address, info.SnapshotConnection)
	}
}

func (p *Pollinosis) ConnectionFailed(info raftio.ConnectionInfo) {
	for _, event := range p.event {
		event.ConnectionFailed(info.Address, info.SnapshotConnection)
	}
}

func (p *Pollinosis) SendSnapshotStarted(_ raftio.SnapshotInfo)   {}
func (p *Pollinosis) SendSnapshotCompleted(_ raftio.SnapshotInfo) {}
func (p *Pollinosis) SendSnapshotAborted(_ raftio.SnapshotInfo)   {}
func (p *Pollinosis) SnapshotReceived(_ raftio.SnapshotInfo)      {}
func (p *Pollinosis) SnapshotRecovered(_ raftio.SnapshotInfo)     {}
func (p *Pollinosis) SnapshotCreated(_ raftio.SnapshotInfo)       {}
func (p *Pollinosis) SnapshotCompacted(_ raftio.SnapshotInfo)     {}
func (p *Pollinosis) LogCompacted(_ raftio.EntryInfo)             {}
func (p *Pollinosis) LogDBCompacted(_ raftio.EntryInfo)           {}
