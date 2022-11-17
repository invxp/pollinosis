package pollinosis

import (
	"fmt"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"path/filepath"
	"sync"
)

type EventListener interface {
	LogUpdated(log []byte, index uint64)
	LogRead(i interface{})
	LeaderUpdated(leaderID, shardID, replicaID, term uint64)
}

type Server struct {
	ReplicaID uint64
	ShardID   uint64

	closed  bool
	join    bool
	members map[uint64]string

	raftConfig config.Config
	hostConfig config.NodeHostConfig

	raft *dragonboat.NodeHost

	event EventListener

	kv sync.Map

	stateMachine interface{}
}

func New(replicaID, shardID uint64, electionRTT, heartbeatRTT, rttMillisecond, snapshotEntries, compactionOverhead uint64, bindAddress, dataDir string, join bool, members map[uint64]string) *Server {
	srv := &Server{ReplicaID: replicaID, ShardID: shardID, join: join, members: members}

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
