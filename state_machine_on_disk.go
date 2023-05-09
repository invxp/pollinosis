package pollinosis

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4/statemachine"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
)

const (
	innerPrefix = "inner_"

	appliedIndexKeyName = "applied_index"

	databaseName = "pollinosis"

	current  = "current"
	updating = "updating"
)

type onDiskStateMachine struct {
	*Pollinosis
	lastApplied atomic.Uint64
	closed      atomic.Bool
	storage     storage
}

func executablePath() string {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return ""
	}

	path, err := filepath.Abs(file)
	if err != nil {
		return ""
	}

	return path[0:strings.LastIndex(path, string(os.PathSeparator))] + string(os.PathSeparator)
}

func uint64ToByte(value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return data
}

func currentDirName(dir string) (string, error) {
	f, err := os.OpenFile(dir, os.O_RDONLY, 0755)
	if err != nil {
		return "", err
	}

	defer func() {
		_ = f.Close()
	}()

	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}

	if len(data) <= 8 {
		return "", errors.New(ErrDataError)
	}

	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err = h.Write(content); err != nil {
		return "", err
	}

	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		return "", errors.New(ErrDataError)
	}

	return string(content), nil
}

func createDirFile(dirFileName string, dirName string) error {
	h := md5.New()
	if _, err := h.Write([]byte(dirName)); err != nil {
		return err
	}

	f, err := os.Create(dirFileName)
	if err != nil {
		return err
	}

	defer func() {
		_ = f.Close()
	}()

	if _, err = f.Write(h.Sum(nil)[:8]); err != nil {
		return err
	}
	if _, err = f.Write([]byte(dirName)); err != nil {
		return err
	}

	return f.Sync()
}

func syncDir(dir string) error {
	//windows are not working
	if runtime.GOOS == "windows" {
		return nil
	}
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return errors.New(ErrNotDir)
	}
	df, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}

	defer func() {
		_ = df.Close()
	}()

	return df.Sync()
}

func cleanDir(dir, excludeDir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != excludeDir {
			if err = os.RemoveAll(toDelete); err != nil {
				return err
			}
		}
	}

	return nil
}

func fileExists(fn string) bool {
	if _, err := os.Stat(fn); err != nil {
		return false
	}
	return true
}

func (sm *onDiskStateMachine) stateMachine(_, _ uint64) statemachine.IOnDiskStateMachine {
	return sm
}

func (sm *onDiskStateMachine) Update(entry []statemachine.Entry) ([]statemachine.Entry, error) {
	if sm.closed.Load() {
		return entry, errors.New(ErrRaftClosed)
	}

	lastAppliedIndex := entry[len(entry)-1].Index

	if sm.lastApplied.Load() >= lastAppliedIndex {
		return entry, errors.New(ErrLastIndex)
	}

	sm.lastApplied.Store(lastAppliedIndex)

	err := sm.storage.Batch(func(batch *pebble.Batch) {
		for i, e := range entry {
			val := &keyValue{}
			if err := json.Unmarshal(e.Cmd, val); err != nil {
				panic("unmarshal data error " + string(e.Cmd))
			}

			sm.event.LogUpdated(val.Key, val.Value, entry[i].Index)

			if err := batch.Set([]byte(val.Key), []byte(val.Value), &pebble.WriteOptions{Sync: false}); err != nil {
				panic("store data error " + val.Key + " " + val.Value + " " + err.Error())
			}
			entry[i].Result = statemachine.Result{Value: uint64(len(entry[i].Cmd))}
		}

		if err := batch.Set([]byte(innerPrefix+appliedIndexKeyName), uint64ToByte(lastAppliedIndex), &pebble.WriteOptions{Sync: false}); err != nil {
			panic("store index error " + err.Error())
		}
	})

	return entry, err
}

func (sm *onDiskStateMachine) Sync() error {
	if sm.closed.Load() {
		return errors.New(ErrRaftClosed)
	}
	return nil
}

func (sm *onDiskStateMachine) SaveSnapshot(_ interface{}, w io.Writer, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return errors.New(ErrRaftClosed)
	}

	sm.storage.mu.RLock()
	defer sm.storage.mu.RUnlock()

	ss := sm.storage.db.NewSnapshot()
	defer func() {
		_ = ss.Close()
	}()

	iter := ss.NewIter(&pebble.IterOptions{})
	defer func() {
		_ = iter.Close()
	}()
	values := make([]*keyValue, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		val := &keyValue{
			Key:   string(iter.Key()),
			Value: string(iter.Value()),
		}
		values = append(values, val)
	}

	if _, err := w.Write(uint64ToByte(uint64(len(values)))); err != nil {
		return err
	}

	for _, kv := range values {
		data, err := json.Marshal(kv)
		if err != nil {
			panic("marshal data error " + err.Error())
		}
		if _, err = w.Write(uint64ToByte(uint64(len(data)))); err != nil {
			panic("write data length error " + err.Error())
		}
		if _, err = w.Write(data); err != nil {
			panic("write data error" + err.Error())
		}
	}

	return nil
}

func (sm *onDiskStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return errors.New(ErrRaftClosed)
	}
	newLastApplied, err := sm.queryAppliedIndex()
	if err != nil {
		return err
	}
	if sm.lastApplied.Load() > newLastApplied {
		return errors.New(ErrLastIndex)
	}
	sm.lastApplied.Store(newLastApplied)

	err = sm.storage.Close()
	if err != nil {
		return err
	}

	dir := filepath.Join(executablePath(), databaseName)
	dir += fmt.Sprintf(".%d.%d", sm.replicaID, sm.shardID)
	databaseDir := filepath.Join(dir, uuid.NewString())
	oldDirName, err := currentDirName(filepath.Join(dir, current))

	if err != nil {
		return err
	}

	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		MaxManifestFileSize: 1024 * 32,
		MemTableSize:        1024 * 32,
		Cache:               cache,
	}
	if err = os.MkdirAll(databaseDir, 0755); err != nil {
		return err
	}
	sm.storage.db, err = pebble.Open(databaseDir, opts)
	if err != nil {
		return err
	}
	cache.Unref()

	sz := make([]byte, 8)
	if _, err = io.ReadFull(r, sz); err != nil {
		return err
	}
	totalSize := binary.LittleEndian.Uint64(sz)

	err = sm.storage.Batch(func(batch *pebble.Batch) {
		for i := uint64(0); i < totalSize; i++ {
			if _, err = io.ReadFull(r, sz); err != nil {
				panic("read data length error " + err.Error())
			}
			toRead := binary.LittleEndian.Uint64(sz)

			data := make([]byte, toRead)
			if _, err = io.ReadFull(r, data); err != nil {
				panic("read data error " + err.Error())
			}
			val := &keyValue{}
			if err = json.Unmarshal(data, val); err != nil {
				panic("unmarshal data length error " + err.Error())
			}
			if err = batch.Set([]byte(val.Key), []byte(val.Value), &pebble.WriteOptions{Sync: false}); err != nil {
				panic("store data error " + err.Error())
			}
		}
	})

	if err = createDirFile(filepath.Join(dir, updating), databaseDir); err != nil {
		return err
	}

	if err = os.Rename(filepath.Join(dir, updating), filepath.Join(dir, current)); err != nil {
		return err
	}

	parent := filepath.Dir(oldDirName)
	if err = os.RemoveAll(oldDirName); err != nil {
		return err
	}

	return syncDir(parent)
}

func (sm *onDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.closed.Load() {
		return nil, errors.New(ErrRaftClosed)
	}
	return &sm.storage, nil
}

func (sm *onDiskStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	var dirName string
	var err error
	var idx uint64

	dir := filepath.Join(executablePath(), databaseName)
	dir += fmt.Sprintf(".%d.%d", sm.replicaID, sm.shardID)

	if err = os.MkdirAll(dir, 0755); err != nil {
		return 0, err
	}

	if err = syncDir(filepath.Dir(dir)); err != nil {
		return 0, err
	}

	currentDirFileName := filepath.Join(dir, current)
	updateDirFileName := filepath.Join(dir, updating)

	if fileExists(currentDirFileName) {
		if dirName, err = currentDirName(currentDirFileName); err != nil {
			return 0, err
		}

		if err = os.RemoveAll(updateDirFileName); err != nil {
			return 0, err
		}

		if err = cleanDir(dir, dirName); err != nil {
			return 0, err
		}
	} else {
		dirName = filepath.Join(dir, uuid.NewString())

		if err = createDirFile(updateDirFileName, dirName); err != nil {
			return 0, err
		}

		if err = syncDir(dir); err != nil {
			return 0, err
		}

		if err = os.Rename(updateDirFileName, currentDirFileName); err != nil {
			return 0, err
		}

		if err = syncDir(dir); err != nil {
			return 0, err
		}
	}

	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		MaxManifestFileSize: 1024 * 32,
		MemTableSize:        1024 * 32,
		Cache:               cache,
	}
	if err = os.MkdirAll(dirName, 0755); err != nil {
		return 0, err
	}
	sm.storage.db, err = pebble.Open(dirName, opts)
	if err != nil {
		return 0, err
	}
	cache.Unref()

	if idx, err = sm.queryAppliedIndex(); err != nil {
		return 0, err
	}

	sm.lastApplied.Store(idx)

	return sm.lastApplied.Load(), nil
}

func (sm *onDiskStateMachine) Lookup(key interface{}) (interface{}, error) {
	if sm.closed.Load() {
		return nil, errors.New(ErrRaftClosed)
	}
	k, ok := key.(string)
	if !ok {
		return nil, errors.New(ErrKeyInvalid)
	}
	sm.event.LogRead(k)
	return sm.storage.Get([]byte(k))
}

func (sm *onDiskStateMachine) Close() error {
	if sm.closed.Load() {
		return errors.New(ErrRaftClosed)
	}
	sm.closed.Store(true)
	return nil
}

func (sm *onDiskStateMachine) queryAppliedIndex() (uint64, error) {
	val, closer, err := sm.storage.db.Get([]byte(innerPrefix + appliedIndexKeyName))
	if err != nil && err != pebble.ErrNotFound {
		return 0, err
	}

	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	if len(val) == 0 {
		return 0, nil
	}

	return binary.LittleEndian.Uint64(val), nil
}
