package pollinosis

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"sync"
)

type storage struct {
	mu sync.RWMutex
	db *pebble.DB
}

func (s *storage) Get(key []byte) ([]byte, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db was not opened")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = closer.Close()
	}()

	return val, nil
}

func (s *storage) Close() error {
	if s.db == nil {
		return fmt.Errorf("db was not opened")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.db.Close()
}

func (s *storage) Batch(f func(batch *pebble.Batch)) error {
	if s.db == nil {
		return fmt.Errorf("db was not opened")
	}

	b := s.db.NewBatch()

	defer func() {
		_ = b.Close()
	}()

	f(b)

	return b.Commit(&pebble.WriteOptions{Sync: true})
}
