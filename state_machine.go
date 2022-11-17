package pollinosis

import (
	"encoding/json"
	"fmt"
	"github.com/lni/dragonboat/v4/statemachine"
	"io"
)

type defaultStateMachine struct {
	*Pollinosis
}

func (sm *defaultStateMachine) stateMachine(_, _ uint64) statemachine.IStateMachine {
	return sm
}

func (sm *defaultStateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	if sm.closed {
		return statemachine.Result{}, fmt.Errorf("raft was closed")
	}
	sm.event.LogUpdated(entry.Cmd, entry.Index)
	val := &kv{}
	if err := json.Unmarshal(entry.Cmd, val); err != nil {
		return statemachine.Result{}, err
	}
	sm.kv.Store(val.Key, val.Value)

	return statemachine.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (sm *defaultStateMachine) Lookup(i interface{}) (interface{}, error) {
	sm.event.LogRead(i)
	val, ok := sm.kv.Load(i.(string))
	if !ok {
		return nil, fmt.Errorf("key: %v not exists", i)
	}
	return val, nil
}

func (sm *defaultStateMachine) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
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

func (sm *defaultStateMachine) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
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

func (sm *defaultStateMachine) Close() error {
	if sm.closed {
		return fmt.Errorf("raft already closed")
	}
	sm.closed = true
	return nil
}
