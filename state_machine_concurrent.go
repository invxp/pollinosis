package pollinosis

import (
	"encoding/json"
	"fmt"
	"github.com/lni/dragonboat/v4/statemachine"
	"io"
)


type concurrentStateMachine struct {
	*Pollinosis
}

func (sm *concurrentStateMachine) stateMachine(_, _ uint64) statemachine.IConcurrentStateMachine {
	return sm
}

func (sm *concurrentStateMachine) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	if sm.closed.Load() {
		return entries, fmt.Errorf("raft was closed")
	}

	for _, entry := range entries {
		sm.event.LogUpdated(entry.Cmd, entry.Index)
		val := &kv{}
		if err := json.Unmarshal(entry.Cmd, val); err != nil {
			return entries, err
		}
		sm.kv.Store(val.Key, val.Value)
	}

	return entries, nil
}

func (sm *concurrentStateMachine) Lookup(i interface{}) (interface{}, error) {
	if sm.closed.Load() {
		return nil, fmt.Errorf("raft was closed")
	}

	sm.event.LogRead(i)
	val, ok := sm.kv.Load(i.(string))
	if !ok {
		return nil, fmt.Errorf("key: %v not exists", i)
	}
	return val, nil
}

func (sm *concurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.closed.Load() {
		return nil, fmt.Errorf("raft was closed")
	}
	return sm, nil
}

func (sm *concurrentStateMachine) SaveSnapshot(i interface{}, writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return fmt.Errorf("raft was closed")
	}
	var lst []kv
	sm.kv.Range(func(key, value any) bool {
		lst = append(lst, kv{key.(string), value.(string)})
		return true
	})

	data, err := json.Marshal(lst)
	if err != nil {
		return err
	}

	_, err = writer.Write(data)

	return err
}

func (sm *concurrentStateMachine) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return fmt.Errorf("raft was closed")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	var lst []kv
	err = json.Unmarshal(data, &lst)
	if err != nil {
		return err
	}
	for _, val := range lst {
		sm.kv.Store(val.Key, val.Value)
	}
	return nil
}

func (sm *concurrentStateMachine) Close() error {
	if sm.closed.Load() {
		return fmt.Errorf("raft already closed")
	}
	sm.closed.Store(true)
	return nil
}

