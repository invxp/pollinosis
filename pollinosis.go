package pollinosis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ErrKeyNotExist   = "key is not exists"
	ErrRaftClosed    = "raft was closed"
	ErrRaftNil       = "raft was nil"
	ErrKeyInvalid    = "key was invalid"
	ErrAlreadyExists = "raft was already exists"
	ErrDBNotOpen     = "db not open"
	ErrDataError     = "data error"
	ErrNotDir        = "not dir"
	ErrLastIndex     = "last index error"
)

// keyValue Raft内部KV数据,用于存储基础数据
type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// EventListener Raft事件监听
type EventListener interface {
	// LogUpdated 触发日志已更新
	LogUpdated(key, value string, index uint64)
	// LogRead 触发日志读取
	LogRead(key string)
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
	closed atomic.Bool
	// join 是否半截新增的节点
	// 如果是新增的节点members必须为空(通过加入的ShardID内获取)
	join bool
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

// New 创建一个Raft实例
func New(replicaID, shardID, electionRTT, heartbeatRTT, rttMillisecond, snapshotEntries, compactionOverhead uint64, bindAddress, dataDir string, join bool, members map[uint64]string) *Pollinosis {
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

	srv.hostConfig = config.NodeHostConfig{
		WALDir:              dir,
		NodeHostDir:         dir,
		RTTMillisecond:      rttMillisecond,
		RaftAddress:         bindAddress,
		SystemEventListener: srv,
		RaftEventListener:   srv,
	}

	return srv
}

// Start 启动Raft实例
func (p *Pollinosis) Start(listener ...EventListener) error {
	p.stateMachine = &defaultStateMachine{p}
	return p.start(listener...)
}

// StartOnDisk 启动Raft实例(PebbleDB)
func (p *Pollinosis) StartOnDisk(listener ...EventListener) error {
	p.stateMachine = &onDiskStateMachine{p, atomic.Uint64{}, atomic.Bool{}, storage{}}
	return p.start(listener...)
}

// StartConcurrent 启动Raft实例(Concurrent)
func (p *Pollinosis) StartConcurrent(listener ...EventListener) error {
	p.stateMachine = &concurrentStateMachine{p}
	return p.start(listener...)
}

// Stop 停止服务
func (p *Pollinosis) Stop() {
	if p.raft == nil {
		return
	}
	p.raft.Close()
	p.raft = nil
}

// Set 从Raft集群内设置KV
// 并不是Leader才可以发起,集群内部任意角色都可以
func (p *Pollinosis) Set(timeout time.Duration, key, value string) error {
	if p.raft == nil {
		return errors.New(ErrRaftNil)
	}

	if len(key) <= 0 {
		return errors.New(ErrKeyInvalid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	session, err := p.raft.SyncGetSession(ctx, p.shardID)
	if err != nil {
		return err
	}
	defer func() {
		_ = p.raft.SyncCloseSession(ctx, session)
	}()
	val := &keyValue{key, value}

	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = p.raft.SyncPropose(ctx, session, bytes)
	if err == nil {
		session.ProposalCompleted()
	}
	return err
}

// Get 从Raft集群内获取KV
// 当前使用的是线性一致性读
// 好处是保证一致性的前提下减缓Leader的读数据的IO压力
func (p *Pollinosis) Get(timeout time.Duration, key string) (value string, err error) {
	if p.raft == nil {
		return "", errors.New(ErrRaftNil)
	}

	if len(key) <= 0 {
		return "", errors.New(ErrKeyInvalid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data, err := p.raft.SyncRead(ctx, p.shardID, key)

	if err != nil {
		return "", err
	}

	switch result := data.(type) {
	case string:
		return result, nil
	case []byte:
		return string(result), nil
	default:
		return "", fmt.Errorf("value data error, key: %v, value: %v", key, data)
	}
}

// NodeInfo 获取集群内所有节点信息
func (p *Pollinosis) NodeInfo() *dragonboat.NodeHostInfo {
	if p.raft == nil {
		return nil
	}
	return p.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
}

// GetValidNodes 获取有效集群成员列表
func (p *Pollinosis) GetValidNodes(timeout time.Duration) (map[uint64]string, error) {
	if p.raft == nil {
		return nil, errors.New(ErrRaftNil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data, err := p.raft.SyncGetShardMembership(ctx, p.shardID)
	if err != nil {
		return nil, err
	}
	return data.Nodes, nil
}

// TransferLeader 切换目标节点为Leader
// 建议通过Leader发起(Follower也可以,只不过内部会多一次请求)
func (p *Pollinosis) TransferLeader(timeout time.Duration, targetReplicaID uint64) error {
	if p.raft == nil {
		return errors.New(ErrRaftNil)
	}

	err := p.raft.RequestLeaderTransfer(p.shardID, targetReplicaID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	var leaderId uint64

	for leaderId != targetReplicaID {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			leaderId, _, _, err = p.raft.GetLeaderID(p.shardID)
		}
	}

	if err == nil && leaderId != targetReplicaID {
		err = fmt.Errorf("leader transfer fail, want: %d, current: %d", targetReplicaID, leaderId)
	}

	return err
}

// AddReplica 从现有集群中新增一个节点
// 被增加的节点New函数的join属性需设置true
// 被增加的节点要在集群内唯一,如果重复了会报错(即使删掉了在重新加也不行 - Raft规范)
func (p *Pollinosis) AddReplica(timeout time.Duration, targetReplicaID uint64, targetAddress string) error {
	if p.raft == nil {
		return errors.New(ErrRaftNil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.raft.SyncRequestAddReplica(ctx, p.shardID, targetReplicaID, targetAddress, 0)
}

// DeleteReplica 从现有集群中删除一个节点
// 被删除的节点是永久性删除,不可以重复添加(Raft规范)
func (p *Pollinosis) DeleteReplica(timeout time.Duration, targetReplicaID uint64) error {
	if p.raft == nil {
		return errors.New(ErrRaftNil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.raft.SyncRequestDeleteReplica(ctx, p.shardID, targetReplicaID, 0)
}

// Ready 集群是否准备完成
// 因为Raft节点之间是需要选举和通信的,所以Service.Start之后需要等待Ready后才可以正常使用
func (p *Pollinosis) Ready(timeout time.Duration) (leaderID uint64, isLeader bool, err error) {
	if p.raft == nil {
		return 0, false, errors.New(ErrRaftNil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	var valid bool

	for !valid {
		select {
		case <-ctx.Done():
			return leaderID, isLeader, ctx.Err()
		default:
			leaderID, _, valid, err = p.raft.GetLeaderID(p.shardID)
		}
	}

	if p.join {
		valid = false
		for !valid {
			select {
			case <-ctx.Done():
				return leaderID, leaderID == p.replicaID, ctx.Err()
			default:
				info := p.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				for _, nodeInfo := range info.ShardInfoList {
					if nodeInfo.ShardID != p.shardID {
						continue
					}
					valid = len(nodeInfo.Nodes) > 0
				}
			}
		}
	}

	return leaderID, leaderID == p.replicaID, err
}

// ShardID 返回集群ID
func (p *Pollinosis) ShardID() uint64 {
	return p.shardID
}

// ReplicaID 返回节点ID
func (p *Pollinosis) ReplicaID() uint64 {
	return p.replicaID
}

// start 启动实际的Raft服务
func (p *Pollinosis) start(listener ...EventListener) error {
	if p.raft != nil {
		return errors.New(ErrAlreadyExists)
	}

	if len(listener) > 0 {
		p.event = listener[0]
	}

	raft, err := dragonboat.NewNodeHost(p.hostConfig)
	if err != nil {
		return err
	}

	p.raft = raft

	switch sm := p.stateMachine.(type) {
	case *defaultStateMachine:
		return p.raft.StartReplica(p.members, p.join, sm.stateMachine, p.raftConfig)
	case *onDiskStateMachine:
		return p.raft.StartOnDiskReplica(p.members, p.join, sm.stateMachine, p.raftConfig)
	case *concurrentStateMachine:
		return p.raft.StartConcurrentReplica(p.members, p.join, sm.stateMachine, p.raftConfig)
	default:
		return fmt.Errorf("unknown driver type: %v", sm)
	}
}
