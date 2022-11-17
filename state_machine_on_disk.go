package pollinosis

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
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
)

const (
	innerPrefix         = "inner_"
	appliedIndexKeyName = "applied_index"

	databaseName = "pollinosis"

	current  = "current"
	updating = "updating"
)

type onDiskStateMachine struct {
	*Server
	lastApplied uint64
	storage     storage
	closed      bool
}

func uint64ToByte(value uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return data
}

func fullPath() string {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		panic(err)
	}

	path, err := filepath.Abs(file)
	if err != nil {
		panic(err)
	}

	return path[0:strings.LastIndex(path, string(os.PathSeparator))] + string(os.PathSeparator)
}

func currentDirName(dir string) string {
	f, err := os.OpenFile(dir, os.O_RDONLY, 0755)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = f.Close(); err != nil {
			panic(err)
		}
	}()

	data, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}

	if len(data) <= 8 {
		panic("corrupted content")
	}

	crc := data[:8]
	content := data[8:]
	h := md5.New()
	if _, err = h.Write(content); err != nil {
		panic(err)
	}

	if !bytes.Equal(crc, h.Sum(nil)[:8]) {
		panic("corrupted content with not matched crc")
	}

	return string(content)
}

func createDirFile(dirFileName string, dirName string) {
	h := md5.New()
	if _, err := h.Write([]byte(dirName)); err != nil {
		panic(err)
	}

	f, err := os.Create(dirFileName)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = f.Close(); err != nil {
			panic(err)
		}
	}()

	if _, err = f.Write(h.Sum(nil)[:8]); err != nil {
		panic(err)
	}
	if _, err = f.Write([]byte(dirName)); err != nil {
		panic(err)
	}
	if err = f.Sync(); err != nil {
		panic(err)
	}
}

func renameDirFile(oldPath, newPath string) {
	if err := os.Rename(oldPath, newPath); err != nil {
		panic(err)
	}
}

func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}

	fileInfo, err := os.Stat(dir)
	if err != nil {
		panic(err)
	}
	if !fileInfo.IsDir() {
		panic("not a dir")
	}
	df, err := os.Open(filepath.Clean(dir))
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = df.Close(); err != nil {
			panic(err)
		}
	}()

	if err = df.Sync(); err != nil {
		panic(err)
	}
}

func cleanDir(dir, excludeDir string) {
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		fmt.Printf("dbdir %s, fi.name %s, dir %s\n", excludeDir, fi.Name(), dir)
		toDelete := filepath.Join(dir, fi.Name())
		if toDelete != excludeDir {
			fmt.Printf("removing %s\n", toDelete)
			if err = os.RemoveAll(toDelete); err != nil {
				panic(err)
			}
		}
	}
}

func fileExists(fn string) bool {
	if _, err := os.Stat(fn); err != nil {
		return false
	}
	return true
}

func (sm *onDiskStateMachine) newStateMachine(_, _ uint64) statemachine.IOnDiskStateMachine {
	return sm
}

func (sm *onDiskStateMachine) Update(entry []statemachine.Entry) ([]statemachine.Entry, error) {
	lastAppliedIndex := entry[len(entry)-1].Index

	if sm.closed {
		panic("update called after Close()")
	}

	if sm.lastApplied >= lastAppliedIndex {
		panic("lastApplied not moving forward")
	}

	sm.lastApplied = lastAppliedIndex

	err := sm.storage.Batch(func(batch *pebble.Batch) {
		for i, e := range entry {
			val := &kv{}
			if err := json.Unmarshal(e.Cmd, val); err != nil {
				panic(err)
			}
			if err := batch.Set([]byte(val.Key), []byte(val.Value), &pebble.WriteOptions{Sync: false}); err != nil {
				panic(err)
			}
			entry[i].Result = statemachine.Result{Value: uint64(len(entry[i].Cmd))}
			if sm.event != nil {
				sm.event.LogUpdated(entry[i].Cmd, entry[i].Index)
			}
		}

		if err := batch.Set([]byte(innerPrefix+appliedIndexKeyName), uint64ToByte(lastAppliedIndex), &pebble.WriteOptions{Sync: false}); err != nil {
			panic(err)
		}
	})

	return entry, err
}

func (sm *onDiskStateMachine) Sync() (err error) {
	return
}

func (sm *onDiskStateMachine) SaveSnapshot(_ interface{}, w io.Writer, _ <-chan struct{}) error {
	if sm.closed {
		panic("prepare snapshot called after Close()")
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
	values := make([]*kv, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		val := &kv{
			Key:   string(iter.Key()),
			Value: string(iter.Value()),
		}
		values = append(values, val)
	}

	if _, err := w.Write(uint64ToByte(uint64(len(values)))); err != nil {
		panic(err)
	}

	for _, kv := range values {
		data, err := json.Marshal(kv)
		if err != nil {
			panic(err)
		}
		if _, err = w.Write(uint64ToByte(uint64(len(data)))); err != nil {
			panic(err)
		}
		if _, err = w.Write(data); err != nil {
			panic(err)
		}
	}

	return nil
}

func (sm *onDiskStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) (err error) {
	if sm.closed {
		panic("recover from snapshot called after Close()")
	}
	newLastApplied := sm.queryAppliedIndex()
	if sm.lastApplied > newLastApplied {
		panic("last applied not moving forward")
	}
	sm.lastApplied = newLastApplied
	_ = sm.storage.Close()

	dir := filepath.Join(fullPath(), fmt.Sprintf("%s.%d.%d", databaseName, sm.ReplicaID, sm.ShardID))
	databaseDir := filepath.Join(dir, uuid.NewString())
	oldDirName := currentDirName(filepath.Join(dir, current))

	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		MaxManifestFileSize: 1024 * 32,
		MemTableSize:        1024 * 32,
		Cache:               cache,
	}
	if err = os.MkdirAll(databaseDir, 0755); err != nil {
		panic(err)
	}
	sm.storage.db, err = pebble.Open(databaseDir, opts)
	if err != nil {
		panic(err)
	}
	cache.Unref()

	sz := make([]byte, 8)
	if _, err = io.ReadFull(r, sz); err != nil {
		panic(err)
	}
	totalSize := binary.LittleEndian.Uint64(sz)

	err = sm.storage.Batch(func(batch *pebble.Batch) {
		for i := uint64(0); i < totalSize; i++ {
			if _, err = io.ReadFull(r, sz); err != nil {
				panic(err)
			}
			toRead := binary.LittleEndian.Uint64(sz)

			data := make([]byte, toRead)
			if _, err = io.ReadFull(r, data); err != nil {
				panic(err)
			}
			val := &kv{}
			if err = json.Unmarshal(data, val); err != nil {
				panic(err)
			}
			if err = batch.Set([]byte(val.Key), []byte(val.Value), &pebble.WriteOptions{Sync: false}); err != nil {
				panic(err)
			}
		}
	})

	createDirFile(filepath.Join(dir, updating), databaseDir)

	renameDirFile(filepath.Join(dir, updating), filepath.Join(dir, current))

	parent := filepath.Dir(oldDirName)
	if err = os.RemoveAll(oldDirName); err != nil {
		panic(err)
	}

	syncDir(parent)

	return
}

func (sm *onDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	if sm.closed {
		panic("prepare snapshot called after Close()")
	}
	return &sm.storage, nil
}

func (sm *onDiskStateMachine) Open(_ <-chan struct{}) (idx uint64, err error) {
	dir := filepath.Join(fullPath(), fmt.Sprintf("%s.%d.%d", databaseName, sm.ReplicaID, sm.ShardID))

	if err = os.MkdirAll(dir, 0755); err != nil {
		panic(err)
	}

	syncDir(filepath.Dir(dir))

	var dirName string

	currentDirFileName := filepath.Join(dir, current)
	updateDirFileName := filepath.Join(dir, updating)

	if fileExists(currentDirFileName) {
		dirName = currentDirName(currentDirFileName)

		if err = os.RemoveAll(updateDirFileName); err != nil {
			panic(err)
		}
		cleanDir(dir, dirName)
	} else {
		dirName = filepath.Join(dir, uuid.NewString())

		createDirFile(updateDirFileName, dirName)
		syncDir(dir)

		renameDirFile(updateDirFileName, currentDirFileName)
		syncDir(dir)
	}

	cache := pebble.NewCache(0)
	opts := &pebble.Options{
		MaxManifestFileSize: 1024 * 32,
		MemTableSize:        1024 * 32,
		Cache:               cache,
	}
	if err = os.MkdirAll(dirName, 0755); err != nil {
		panic(err)
	}
	sm.storage.db, err = pebble.Open(dirName, opts)
	if err != nil {
		panic(err)
	}
	cache.Unref()

	sm.lastApplied = sm.queryAppliedIndex()

	return sm.lastApplied, nil
}

func (sm *onDiskStateMachine) Lookup(key interface{}) (value interface{}, err error) {
	if sm.closed {
		return nil, fmt.Errorf("raft was closed")
	}
	if sm.event != nil {
		sm.event.LogRead(key)
	}
	k, _ := key.(string)
	return sm.storage.Get([]byte(k))
}

func (sm *onDiskStateMachine) Close() (err error) {
	sm.closed = true
	return sm.storage.Close()
}

func (sm *onDiskStateMachine) queryAppliedIndex() uint64 {
	val, closer, err := sm.storage.db.Get([]byte(innerPrefix + appliedIndexKeyName))
	if err != nil && err != pebble.ErrNotFound {
		panic(err)
	}

	defer func() {
		if closer != nil {
			_ = closer.Close()
		}
	}()

	if len(val) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(val)
}
