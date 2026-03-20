package lsm

import (
	"sync"
	"sync/atomic"
)

// IsolationLevel defines the transaction isolation level.
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// TranManager allocates transaction IDs and tracks commit state.
type TranManager struct {
	mu            sync.Mutex
	engine        *LSMEngine
	nextID        uint64
	maxFinishedID uint64
	active        map[uint64]*TranContext
}

// NewTranManager creates a transaction manager for an engine.
func NewTranManager(engine *LSMEngine) *TranManager {
	return &TranManager{
		engine: engine,
		active: make(map[uint64]*TranContext),
	}
}

func (tm *TranManager) getNextTransactionId() uint64 {
	return atomic.AddUint64(&tm.nextID, 1)
}

func (tm *TranManager) maxFinished() uint64 {
	return atomic.LoadUint64(&tm.maxFinishedID)
}

func (tm *TranManager) updateMaxFinished(id uint64) {
	for {
		cur := atomic.LoadUint64(&tm.maxFinishedID)
		if id <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&tm.maxFinishedID, cur, id) {
			return
		}
	}
}

// Begin starts a new transaction.
func (tm *TranManager) Begin(level IsolationLevel) *TranContext {
	trancID := tm.getNextTransactionId()
	ctx := &TranContext{
		engine:      tm.engine,
		manager:     tm,
		trancID:     trancID,
		isolation:   level,
		startID:     tm.maxFinished(),
		temp:        make(map[string]*string),
		readMap:     make(map[string]readRecord),
		rollbackMap: make(map[string]readRecord),
	}

	tm.mu.Lock()
	tm.active[trancID] = ctx
	tm.mu.Unlock()

	return ctx
}

// TranContext is a transaction context.
type TranContext struct {
	engine    *LSMEngine
	manager   *TranManager
	trancID   uint64
	startID   uint64
	isolation IsolationLevel

	temp        map[string]*string
	readMap     map[string]readRecord
	rollbackMap map[string]readRecord

	committed bool
	aborted   bool
}

type readRecord struct {
	valid   bool
	value   string
	trancID uint64
}

func (tc *TranContext) Put(key string, value string) {
	if tc.committed || tc.aborted {
		return
	}

	if tc.isolation == ReadUncommitted {
		if _, ok := tc.rollbackMap[key]; !ok {
			v, tid := tc.engine.Get(key, 0)
			tc.rollbackMap[key] = readRecord{valid: tid != 0, value: v, trancID: tid}
		}
		tc.engine.Put(key, value, tc.trancID)
		return
	}

	v := value
	tc.temp[key] = &v
}

func (tc *TranContext) Remove(key string) {
	if tc.committed || tc.aborted {
		return
	}

	if tc.isolation == ReadUncommitted {
		if _, ok := tc.rollbackMap[key]; !ok {
			v, tid := tc.engine.Get(key, 0)
			tc.rollbackMap[key] = readRecord{valid: tid != 0, value: v, trancID: tid}
		}
		tc.engine.Remove(key, tc.trancID)
		return
	}

	tc.temp[key] = nil
}

func (tc *TranContext) Get(key string) (string, bool) {
	if tc.committed || tc.aborted {
		return "", false
	}

	if v, ok := tc.temp[key]; ok {
		if v == nil {
			return "", false
		}
		return *v, true
	}

	switch tc.isolation {
	case ReadUncommitted:
		val, tid := tc.engine.Get(key, 0)
		if tid == 0 {
			return "", false
		}
		return val, true
	case ReadCommitted:
		val, tid := tc.engine.Get(key, tc.manager.maxFinished())
		if tid == 0 {
			return "", false
		}
		return val, true
	case RepeatableRead, Serializable:
		if rec, ok := tc.readMap[key]; ok {
			if !rec.valid {
				return "", false
			}
			return rec.value, true
		}
		val, tid := tc.engine.Get(key, tc.startID)
		rec := readRecord{valid: tid != 0, value: val, trancID: tid}
		tc.readMap[key] = rec
		if !rec.valid {
			return "", false
		}
		return val, true
	default:
		return "", false
	}
}

func (tc *TranContext) Commit() bool {
	if tc.committed || tc.aborted {
		return false
	}

	// Serialize commit path to simplify conflict checks.
	tc.manager.mu.Lock()
	defer tc.manager.mu.Unlock()

	switch tc.isolation {
	case ReadUncommitted:
		tc.manager.updateMaxFinished(tc.trancID)
		tc.committed = true
		delete(tc.manager.active, tc.trancID)
		return true
	case ReadCommitted:
		// no validation
	case RepeatableRead, Serializable:
		// Validate reads against current committed state.
		committed := tc.manager.maxFinished()
		for k, rec := range tc.readMap {
			val, tid := tc.engine.Get(k, committed)
			if rec.valid {
				if tid == 0 || tid != rec.trancID || val != rec.value {
					tc.aborted = true
					delete(tc.manager.active, tc.trancID)
					return false
				}
			} else {
				if tid != 0 {
					tc.aborted = true
					delete(tc.manager.active, tc.trancID)
					return false
				}
			}
		}
		// Optional write-write check
		for k := range tc.temp {
			_, tid := tc.engine.Get(k, committed)
			if tid > tc.startID {
				tc.aborted = true
				delete(tc.manager.active, tc.trancID)
				return false
			}
		}
	}

	// Apply buffered writes.
	for k, v := range tc.temp {
		if v == nil {
			tc.engine.Remove(k, tc.trancID)
		} else {
			tc.engine.Put(k, *v, tc.trancID)
		}
	}

	tc.manager.updateMaxFinished(tc.trancID)
	tc.committed = true
	delete(tc.manager.active, tc.trancID)
	return true
}

func (tc *TranContext) Abort() bool {
	if tc.committed || tc.aborted {
		return false
	}

	if tc.isolation == ReadUncommitted {
		// Rollback by restoring previous values with a new transaction id.
		rollbackID := tc.manager.getNextTransactionId()
		for k, rec := range tc.rollbackMap {
			if rec.valid {
				tc.engine.Put(k, rec.value, rollbackID)
			} else {
				tc.engine.Remove(k, rollbackID)
			}
		}
		tc.manager.updateMaxFinished(rollbackID)
	}

	tc.aborted = true
	tc.manager.mu.Lock()
	delete(tc.manager.active, tc.trancID)
	tc.manager.mu.Unlock()
	return true
}

