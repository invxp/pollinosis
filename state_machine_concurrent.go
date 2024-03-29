package pollinosis

import (
	"encoding/json"
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
		return entries, ErrRaftClosed
	}

	for _, entry := range entries {
		val := &keyValue{}
		if err := json.Unmarshal(entry.Cmd, val); err != nil {
			return entries, err
		}

		sm.kv.Store(val.Key, val.Value)

		for _, event := range sm.event {
			event.LogUpdated(val.Key, val.Value.Value, entry.Index)
		}
	}

	return entries, nil
}

func (sm *concurrentStateMachine) Lookup(i interface{}) (interface{}, error) {
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

func (sm *concurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.closed.Load() {
		return nil, ErrRaftClosed
	}
	return sm, nil
}

func (sm *concurrentStateMachine) SaveSnapshot(_ interface{}, writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
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

func (sm *concurrentStateMachine) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
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

func (sm *concurrentStateMachine) Close() error {
	if sm.closed.Load() {
		return ErrRaftClosed
	}
	sm.closed.Store(true)
	return nil
}
