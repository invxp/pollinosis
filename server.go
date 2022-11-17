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

func (s *Server) Start(listener ...EventListener) error {
	if s.raft != nil {
		return fmt.Errorf("raft was already started")
	}

	if len(listener) > 0 {
		s.event = listener[0]
	}

	raft, err := dragonboat.NewNodeHost(s.hostConfig)
	if err != nil {
		return err
	}

	s.raft = raft

	s.stateMachine = &defaultStateMachine{s}

	return s.raft.StartReplica(s.members, s.join, s.stateMachine.(*defaultStateMachine).stateMachine, s.raftConfig)
}

func (s *Server) StartOnDisk(listener ...EventListener) error {
	if s.raft != nil {
		return fmt.Errorf("raft was already started")
	}

	if len(listener) > 0 {
		s.event = listener[0]
	}

	raft, err := dragonboat.NewNodeHost(s.hostConfig)
	if err != nil {
		return err
	}

	s.raft = raft

	s.stateMachine = &onDiskStateMachine{s, 0, storage{}, false}

	return s.raft.StartOnDiskReplica(s.members, s.join, s.stateMachine.(*onDiskStateMachine).newStateMachine, s.raftConfig)
}

func (s *Server) Stop() {
	if s.raft == nil {
		return
	}
	s.raft.Close()
	s.raft = nil
}

func (s *Server) Ready(timeout time.Duration) (leaderId uint64, isLeader bool, err error) {
	if s.raft == nil {
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
			leaderId, _, valid, err = s.raft.GetLeaderID(s.ShardID)
		}
	}

	if s.join {
		valid = false
		for !valid {
			select {
			case <-ctx.Done():
				return leaderId, leaderId == s.ReplicaID, ctx.Err()
			default:
				info := s.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				for _, nodeInfo := range info.ShardInfoList {
					if nodeInfo.ShardID != s.ShardID {
						continue
					}
					valid = len(nodeInfo.Nodes) > 0
				}
			}
		}
	}

	return leaderId, leaderId == s.ReplicaID, err
}

func (s *Server) Set(timeout time.Duration, key, value string) error {
	if s.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return fmt.Errorf("key length must>0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	session := s.raft.GetNoOPSession(s.ShardID)
	val := &kv{key, value}

	bytes, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}

	_, err = s.raft.SyncPropose(ctx, session, bytes)
	return err
}

func (s *Server) Get(timeout time.Duration, key string) (string, error) {
	if s.raft == nil {
		return "", fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return "", fmt.Errorf("key length must>0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data, err := s.raft.SyncRead(ctx, s.ShardID, key)

	if err != nil {
		return "", err
	}

	switch result := data.(type) {
	case string:
		return result, nil
	case []byte:
		return string(result), nil
	default:
		return "", fmt.Errorf("value data error, key: %s, value: %v", key, data)
	}
}

func (s *Server) NodeInfo() *dragonboat.NodeHostInfo {
	if s.raft == nil {
		return nil
	}
	return s.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
}

func (s *Server) TransferLeader(timeout time.Duration, targetReplicaID uint64) error {
	if s.raft == nil {
		return fmt.Errorf("raft was nil")
	}
	err := s.raft.RequestLeaderTransfer(s.ShardID, targetReplicaID)
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
			leaderId, _, _, err = s.raft.GetLeaderID(s.ShardID)
		}
	}

	if err == nil && leaderId != targetReplicaID {
		err = fmt.Errorf("leader transfer fail, want: %d, current: %d", targetReplicaID, leaderId)
	}

	return err
}

func (s *Server) AddReplica(timeout time.Duration, replicaID uint64, target string, configChangeIndex uint64) error {
	if s.raft == nil {
		return fmt.Errorf("raft was nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.raft.SyncRequestAddReplica(ctx, s.ShardID, replicaID, target, configChangeIndex)
}

func (s *Server) DeleteReplica(timeout time.Duration, replicaID uint64, configChangeIndex uint64) error {
	if s.raft == nil {
		return fmt.Errorf("raft was nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.raft.SyncRequestDeleteReplica(ctx, s.ShardID, replicaID, configChangeIndex)
}
