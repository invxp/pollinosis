package pollinosis

import (
	"encoding/json"
	"errors"
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
		return entries, errors.New(ErrRaftClosed)
	}

	for _, entry := range entries {
		val := &keyValue{}
		if err := json.Unmarshal(entry.Cmd, val); err != nil {
			return entries, err
		}

		sm.event.LogUpdated(val.Key, val.Value, entry.Index)

		sm.kv.Store(val.Key, val.Value)
	}

	return entries, nil
}

func (sm *concurrentStateMachine) Lookup(i interface{}) (interface{}, error) {
	if sm.closed.Load() {
		return nil, errors.New(ErrRaftClosed)
	}

	key, ok := i.(string)

	if !ok {
		return nil, errors.New(ErrKeyInvalid)
	}

	val, exists := sm.kv.Load(key)

	defer sm.event.LogRead(key)

	if !exists {
		return nil, errors.New(ErrKeyNotExist)
	}
	return val, nil
}

func (sm *concurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.closed.Load() {
		return nil, errors.New(ErrRaftClosed)
	}
	return sm, nil
}

func (sm *concurrentStateMachine) SaveSnapshot(_ interface{}, writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return errors.New(ErrRaftClosed)
	}
	var lst []keyValue
	sm.kv.Range(func(key, value any) bool {
		lst = append(lst, keyValue{key.(string), value.(string)})
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
		return errors.New(ErrRaftClosed)
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
		return errors.New(ErrRaftClosed)
	}
	sm.closed.Store(true)
	return nil
}
