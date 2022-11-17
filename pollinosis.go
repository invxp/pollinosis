package pollinosis

import (
	"fmt"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"path/filepath"
	"sync"
)

// EventListener Raft事件监听
type EventListener interface {
	// LogUpdated 触发日志已更新
	LogUpdated(log []byte, index uint64)
	// LogRead 触发日志读取
	LogRead(i interface{})
	// LeaderUpdated Leader更新
	LeaderUpdated(leaderID, shardID, replicaID, term uint64)

	// NodeShuttingDown 节点关闭中
	NodeShuttingDown()
	// NodeUnloaded 节点被卸载
	NodeUnloaded(replicaID, shardID uint64)
	// NodeDeleted 节点被删除
	NodeDeleted(replicaID, shardID uint64)
	// NodeReady 节点可正常服务
	NodeReady(replicaID, shardID uint64)
	// MembershipChanged 集群内节点成员发生变化
	MembershipChanged(replicaID, shardID uint64)

	// ConnectionEstablished 已建立Raft连接
	ConnectionEstablished(address string, snapshot bool)
	// ConnectionFailed 建立Raft连接失败
	ConnectionFailed(address string, snapshot bool)
}

// Pollinosis 服务器主要结构
type Pollinosis struct {
	// replicaID 节点ID, 在同一个集群内要保证唯一
	replicaID uint64
	// shardID 集群ID, 可以存在多个集群
	// 例如三个节点ID:1,2,3,如果想搭建在同一个集群则同时设置集群ID为100
	// 1,100
	// 2,100
	// 3,100
	shardID uint64

	// closed 是否已关闭
	closed  bool
	// join 是否半截新增的节点
	// 如果是新增的节点members必须为空(通过加入的ShardID内获取)
	join    bool
	// 初始化集群ID+IP地址列表
	// 例如三个节点ID:1,2,3,如果想搭建在同一个集群则同时设置集群ID为100
	// 1,100
	// 2,100
	// 3,100
	// 该结构为[1:"192.168.0.1", 2:"192.168.0.2", 3:"192.168.0.3"]
	members map[uint64]string

	// raftConfig Raft配置
	// v4版dragon boat支持preVote
	// 详见Config内注释
	raftConfig config.Config
	// hostConfig 节点配置,详见Config内注释
	hostConfig config.NodeHostConfig

	// raft Raft实例对象
	raft *dragonboat.NodeHost

	// event 事件监听器,有一个默认的,可自定义实现
	event EventListener

	// kv 用于持久化数据
	kv sync.Map

	// stateMachine 状态机,目前支持两种持久化方案(普通Map与PebbleDB)
	stateMachine interface{}
}

type defaultEventListener struct{}
func (d defaultEventListener) LogUpdated(_ []byte, _ uint64) {}
func (d defaultEventListener) LogRead(_ interface{}) {}
func (d defaultEventListener) LeaderUpdated(_, _, _, _ uint64) {}
func (d defaultEventListener) NodeShuttingDown() {}
func (d defaultEventListener) NodeUnloaded(_, _ uint64) {}
func (d defaultEventListener) NodeDeleted(_, _ uint64) {}
func (d defaultEventListener) NodeReady(_, _ uint64) {}
func (d defaultEventListener) MembershipChanged(_, _ uint64) {}
func (d defaultEventListener) ConnectionEstablished(_ string, _ bool) {}
func (d defaultEventListener) ConnectionFailed(_ string, _ bool) {}

var defaultEvent = defaultEventListener{}

type nodeHostEvent struct{*Pollinosis}

// New 创建一个Raft实例
func New(replicaID, shardID uint64, electionRTT, heartbeatRTT, rttMillisecond, snapshotEntries, compactionOverhead uint64, bindAddress, dataDir string, join bool, members map[uint64]string) *Pollinosis {
	srv := &Pollinosis{replicaID: replicaID, shardID: shardID, join: join, members: members, event: defaultEvent}

	srv.raftConfig = config.Config{
		ReplicaID:          replicaID,
		ShardID:            shardID,
		CheckQuorum:        true,
		ElectionRTT:        electionRTT,
		HeartbeatRTT:       heartbeatRTT,
		SnapshotEntries:    snapshotEntries,
		CompactionOverhead: compactionOverhead,
	}

	dir := filepath.Join(dataDir, fmt.Sprintf("%d", replicaID))

	var nhEvent = &nodeHostEvent{srv}

	srv.hostConfig = config.NodeHostConfig{
		WALDir:              dir,
		NodeHostDir:         dir,
		RTTMillisecond:      rttMillisecond,
		RaftAddress:         bindAddress,
		SystemEventListener: nhEvent,
		RaftEventListener:   nhEvent,
	}

	return srv
}
