package pollinosis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lni/dragonboat/v4"
	"time"
)

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Start 启动Raft实例
func (p *Pollinosis) Start(listener ...EventListener) error {
	p.stateMachine = &defaultStateMachine{p}
	return p.start(listener...)
}

// StartOnDisk 启动Raft实例(PebbleDB)
func (p *Pollinosis) StartOnDisk(listener ...EventListener) error {
	p.stateMachine = &onDiskStateMachine{p, 0, storage{}, false}
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

// ShardID 返回集群ID
func (p *Pollinosis) ShardID() uint64 {
	return p.shardID
}

// ReplicaID 返回节点ID
func (p *Pollinosis) ReplicaID() uint64 {
	return p.replicaID
}

// Ready 是否已经加入到集群内并可以进行服务
func (p *Pollinosis) Ready(timeout time.Duration) (leaderId uint64, isLeader bool, err error) {
	if p.raft == nil {
		return leaderId, isLeader, fmt.Errorf("raft was nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	var valid bool

	for !valid {
		select {
		case <-ctx.Done():
			return leaderId, isLeader, ctx.Err()
		default:
			leaderId, _, valid, err = p.raft.GetLeaderID(p.shardID)
		}
	}

	if p.join {
		valid = false
		for !valid {
			select {
			case <-ctx.Done():
				return leaderId, leaderId == p.replicaID, ctx.Err()
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

	return leaderId, leaderId == p.replicaID, err
}

// Set 设置KV
// 并不是Leader才可以发起,集群内部任意角色都可以
func (p *Pollinosis) Set(timeout time.Duration, key, value string) error {
	if p.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return fmt.Errorf("key length must>0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	session := p.raft.GetNoOPSession(p.shardID)
	val := &kv{key, value}

	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = p.raft.SyncPropose(ctx, session, bytes)
	return err
}

// Get 获取KV
// 当前使用的是线性一致性读
// 好处是保证一致性的前提下减缓Leader的读数据的IO压力
func (p *Pollinosis) Get(timeout time.Duration, key string) (string, error) {
	if p.raft == nil {
		return "", fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return "", fmt.Errorf("key length must>0")
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

// TransferLeader 切换Leader
// 建议通过Leader发起
func (p *Pollinosis) TransferLeader(timeout time.Duration, targetReplicaID uint64) error {
	if p.raft == nil {
		return fmt.Errorf("raft was nil")
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

// AddReplica 新增一个节点
// 被增加的节点join属性需设置true
func (p *Pollinosis) AddReplica(timeout time.Duration, replicaID uint64, target string, configChangeIndex uint64) error {
	if p.raft == nil {
		return fmt.Errorf("raft was nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.raft.SyncRequestAddReplica(ctx, p.shardID, replicaID, target, configChangeIndex)
}

// DeleteReplica 删除一个节点
func (p *Pollinosis) DeleteReplica(timeout time.Duration, replicaID uint64, configChangeIndex uint64) error {
	if p.raft == nil {
		return fmt.Errorf("raft was nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.raft.SyncRequestDeleteReplica(ctx, p.shardID, replicaID, configChangeIndex)
}

// start 启动实际的Raft服务
func (p *Pollinosis) start(listener... EventListener) error {
	if p.raft != nil {
		return fmt.Errorf("raft was already started")
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
	default:
		return fmt.Errorf("unknown driver type: %v", sm)
	}
}