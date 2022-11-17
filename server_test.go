package pollinosis

import (
	"context"
	"fmt"
	"github.com/lni/dragonboat/v4"
	"os"
	"sync"
	"testing"
	"time"
)

func TestServer_StartAndReady(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}
}

func TestServer_StartOnDiskAndReadyGetSet(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.StartOnDisk(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	var value string
	want := "Value"
	if err = servers[leaderID-1].Set(time.Second*10, "Key", want); err != nil {
		t.Fatal(err)
	}
	if value, err = servers[leaderID-1].Get(time.Second*10, "Key"); err != nil || value != want {
		t.Fatal("value was diff", err, value, want)
	}
}

func TestServer_Stop(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	for _, server := range servers {
		server.Stop()
	}
}

func TestServer_GetSet(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1
	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	wantValue := "Value"

	var value string
	if value, err = servers[leader].Get(time.Second*10, "Key"); err == nil {
		t.Fatal("value must be nil")
	}

	err = servers[follower].Set(time.Second*10, "Key", wantValue)
	if err != nil {
		t.Fatal(err)
	}

	if value, err = servers[leader].Get(time.Second*10, "Key"); err != nil || value != wantValue {
		t.Fatal(err, value, wantValue)
	}
}

func TestServer_TransferLeader(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1
	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	err = servers[follower].TransferLeader(time.Second*5, servers[follower].ReplicaID)
	if err != nil {
		t.Fatal(err)
	}

	if newLeader, _, err := servers[follower].Ready(time.Second * 5); err != nil {
		t.Fatal(err)
	} else {
		if newLeader != servers[follower].ReplicaID {
			t.Fatal("leaderID transfer failed", "current", newLeader, "want", servers[follower].ReplicaID)
		}
	}
}

func TestServer_AddRemoveNodeAndGetValue(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1
	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	wantValue := "Value"

	err = servers[follower].Set(time.Second*10, "Key", wantValue)
	if err != nil {
		t.Fatal(err)
	}

	var value string
	if value, err = servers[leader].Get(time.Second*10, "Key"); err != nil || value != wantValue {
		t.Fatal(err, value, wantValue)
	}

	newServer := New(
		total+1,
		100,
		10,
		1,
		200,
		0,
		100,
		fmt.Sprintf("0.0.0.0:%d", 10000+total+1),
		fmt.Sprintf("raft_%d", total+1),
		true,
		nil,
	)

	_ = os.RemoveAll(fmt.Sprintf("raft_%d", total+1))

	err = newServer.Start()
	if err != nil {
		t.Fatal(err)
	}

	err = servers[leader].AddReplica(time.Second*10, newServer.ReplicaID, newServer.hostConfig.RaftAddress, 0)
	if err != nil {
		t.Fatal(err)
	}

	newLeaderID, _, err := newServer.Ready(time.Second * 10)
	if err != nil {
		t.Fatal(err)
	}

	if newLeaderID != leaderID {
		t.Fatal("leaderID was diff", "want", leaderID, "current", newLeaderID)
	}

	if len(servers[leader].NodeInfo().ShardInfoList[0].Nodes) != 4 {
		t.Fatal("nodes diff", servers[leader].NodeInfo().ShardInfoList[0])
	}

	if value, err = newServer.Get(time.Second*10, "Key"); err != nil || value != wantValue {
		t.Fatal(err, value, wantValue)
	}

	err = newServer.DeleteReplica(time.Second*10, newServer.ReplicaID, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(servers[leader].NodeInfo().ShardInfoList[0].Nodes) != 3 {
		t.Fatal("nodes diff", servers[leader].NodeInfo().ShardInfoList[0])
	}

	newServer.Stop()
}

func TestServer_Snapshot(t *testing.T) {
	var servers []*Server

	total := uint64(3)
	members := make(map[uint64]string)

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		members[id] = fmt.Sprintf("0.0.0.0:%d", address)
		//_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		servers = append(servers, New(
			id,
			100,
			10,
			1,
			200,
			0,
			100,
			members[id],
			fmt.Sprintf("raft_%d", id),
			false,
			members,
		))
	}

	for _, srv := range servers {
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
	}

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(len(servers))

	for _, server := range servers {
		go func(server *Server) {
			defer wg.Done()
			leaderID, _, err = server.Ready(time.Second * 5)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
			t.Log("leaderID voted:", checkLeaders)
		}(server)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1
	for i := 0; i < 1000; i++ {
		if err = servers[leader].Set(time.Second*10, fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	_, err = servers[leader].raft.SyncRequestSnapshot(ctx, servers[leader].ShardID, dragonboat.SnapshotOption{})

	if err != nil {
		t.Fatal(err)
	}

}
