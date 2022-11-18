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

func TestDefault_StartAndReady(t *testing.T) {
	var servers []*Pollinosis

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
		go func(server *Pollinosis) {
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

	for _, srv := range servers {
		srv.Stop()
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}

func TestDefault_StartAndReadyToListener(t *testing.T) {
	var servers []*Pollinosis

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

	cs := CustomListener{}
	for _, srv := range servers {
		if err := srv.Start(&cs); err != nil {
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
		go func(server *Pollinosis) {
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

	for _, srv := range servers {
		srv.Stop()
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}

func TestDefault_GetSet(t *testing.T) {
	var servers []*Pollinosis

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
		go func(server *Pollinosis) {
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

	for _, srv := range servers {
		srv.Stop()
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}

func TestDefault_TransferLeader(t *testing.T) {
	var servers []*Pollinosis

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
		go func(server *Pollinosis) {
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

	err = servers[leader].TransferLeader(time.Second*5, servers[follower].replicaID)
	if err != nil {
		if err != context.DeadlineExceeded {
			t.Fatal(err)
		}
		t.Log("leaderID transfer wait failed", err)
	}

	if newLeader, _, err := servers[follower].Ready(time.Second * 5); err != nil {
		t.Fatal(err)
	} else {
		if newLeader != servers[follower].replicaID {
			t.Log("leaderID transfer failed warning", "current", newLeader, "want", servers[follower].replicaID)
		}
	}

	for _, srv := range servers {
		srv.Stop()
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}

func TestDefault_AddRemoveNodeAndGetValue(t *testing.T) {
	var servers []*Pollinosis

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
		go func(server *Pollinosis) {
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

	err = servers[leader].AddReplica(time.Second*10, newServer.replicaID, newServer.hostConfig.RaftAddress, 0)
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

	err = newServer.DeleteReplica(time.Second*10, newServer.replicaID, 0)
	if err != nil {
		t.Log("delete replica error", err)
	}

	if len(servers[leader].NodeInfo().ShardInfoList[0].Nodes) != 3 {
		t.Fatal("nodes diff", servers[leader].NodeInfo().ShardInfoList[0])
	}

	newServer.Stop()

	for _, srv := range servers {
		srv.Stop()
	}

	_ = os.RemoveAll(fmt.Sprintf("raft_4"))

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}

func TestDefault_Snapshot(t *testing.T) {
	var servers []*Pollinosis

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
		go func(server *Pollinosis) {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()

	_, err = servers[leader].raft.SyncRequestSnapshot(ctx, servers[leader].shardID, dragonboat.SnapshotOption{})

	if err != nil {
		t.Fatal(err)
	}

	for _, srv := range servers {
		srv.Stop()
	}

	for id, address := uint64(1), uint64(10001); id <= total; id, address = id+1, address+1 {
		_ = os.RemoveAll(fmt.Sprintf("raft_%d", id))
	}
}
