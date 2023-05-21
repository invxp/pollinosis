package pollinosis

import (
	"encoding/json"
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
	if sm.closed.Load() {
		return statemachine.Result{}, ErrRaftClosed
	}
	val := &keyValue{}
	if err := json.Unmarshal(entry.Cmd, val); err != nil {
		return statemachine.Result{}, err
	}
	sm.kv.Store(val.Key, val.Value)

	for _, event := range sm.event {
		event.LogUpdated(val.Key, val.Value.Value, entry.Index)
	}

	return statemachine.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (sm *defaultStateMachine) Lookup(i interface{}) (interface{}, error) {
	if sm.closed.Load() {
		return nil, ErrRaftClosed
	}

	key, ok := i.(string)
	if !ok {
		return nil, ErrKeyInvalid
	}

	val, exists := sm.kv.Load(key)

	for _, event := range sm.event {
		event.LogRead(key)
	}

	if !exists {
		return nil, ErrKeyNotExist
	}
	return val, nil
}

func (sm *defaultStateMachine) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return ErrRaftClosed
	}
	var lst []keyValue
	sm.kv.Range(func(key, value any) bool {
		lst = append(lst, keyValue{key.(string), value.(values)})
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
	if sm.closed.Load() {
		return ErrRaftClosed
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	var lst []keyValue
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
	if sm.closed.Load() {
		return ErrRaftClosed
	}
	sm.closed.Store(true)
	return nil
}
