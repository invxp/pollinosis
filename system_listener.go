package pollinosis

import (
	"github.com/lni/dragonboat/v4/raftio"
)

func (s *Server) NodeHostShuttingDown() {
	//TODO
}

func (s *Server) NodeUnloaded(info raftio.NodeInfo) {
	//TODO
}

func (s *Server) NodeDeleted(info raftio.NodeInfo) {
	//TODO
}

func (s *Server) NodeReady(info raftio.NodeInfo) {
	//TODO
}

func (s *Server) MembershipChanged(info raftio.NodeInfo) {
	//TODO
}

func (s *Server) ConnectionEstablished(info raftio.ConnectionInfo) {
	//TODO
}

func (s *Server) ConnectionFailed(info raftio.ConnectionInfo) {
	//TODO
}

func (s *Server) SendSnapshotStarted(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SendSnapshotCompleted(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SendSnapshotAborted(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SnapshotReceived(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SnapshotRecovered(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SnapshotCreated(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) SnapshotCompacted(info raftio.SnapshotInfo) {
	//TODO
}

func (s *Server) LogCompacted(info raftio.EntryInfo) {
	//TODO
}

func (s *Server) LogDBCompacted(info raftio.EntryInfo) {
	//TODO
}

func (s *Server) LeaderUpdated(info raftio.LeaderInfo) {
	if s.event != nil {
		s.event.LeaderUpdated(info.LeaderID, info.ShardID, info.ReplicaID, info.Term)
	}
}
