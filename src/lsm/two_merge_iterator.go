package lsm

type KVID struct {
	Key     string
	Value   string
	TrancID uint64
}

type TwoMergeIterator struct {
	itA        Iterator
	itB        Iterator
	chooseA    bool
	currentKV  *KVID
	maxTrancID uint64
}

// NewTwoMergeIterator merges two iterators (A has higher priority on equal keys).
func NewTwoMergeIterator(a, b Iterator, maxID uint64) *TwoMergeIterator {
	it := &TwoMergeIterator{
		itA:        a,
		itB:        b,
		maxTrancID: maxID,
	}
	it.UpdateCurrent()
	return it
}

// ChooseItA decides whether to pick A at current position.
func (it *TwoMergeIterator) ChooseItA() bool {
	if it.itA == nil || !it.itA.Valid() {
		return false
	}
	if it.itB == nil || !it.itB.Valid() {
		return true
	}

	keyA := it.itA.Key()
	keyB := it.itB.Key()
	if keyA < keyB {
		return true
	}
	if keyA > keyB {
		return false
	}
	return true
}

// UpdateCurrent refreshes the cached current KV.
func (it *TwoMergeIterator) UpdateCurrent() {
	it.skip_by_tranc_id()
	if (it.itA == nil || !it.itA.Valid()) && (it.itB == nil || !it.itB.Valid()) {
		it.currentKV = nil
		return
	}

	it.chooseA = it.ChooseItA()
	if it.chooseA {
		it.currentKV = &KVID{
			Key:     it.itA.Key(),
			Value:   it.itA.Value(),
			TrancID: it.itA.TrancID(),
		}
	} else {
		it.currentKV = &KVID{
			Key:     it.itB.Key(),
			Value:   it.itB.Value(),
			TrancID: it.itB.TrancID(),
		}
	}
}

// skip_by_tranc_id skips entries not visible to maxTrancID.
func (it *TwoMergeIterator) skip_by_tranc_id() {
	if it.maxTrancID == 0 {
		return
	}
	for it.itA != nil && it.itA.Valid() && it.itA.TrancID() > it.maxTrancID {
		it.itA.Next()
	}
	for it.itB != nil && it.itB.Valid() && it.itB.TrancID() > it.maxTrancID {
		it.itB.Next()
	}
}

func (it *TwoMergeIterator) Valid() bool { return it.currentKV != nil }

func (it *TwoMergeIterator) Key() string {
	if it.currentKV == nil {
		return ""
	}
	return it.currentKV.Key
}

func (it *TwoMergeIterator) Value() string {
	if it.currentKV == nil {
		return ""
	}
	return it.currentKV.Value
}

func (it *TwoMergeIterator) TrancID() uint64 {
	if it.currentKV == nil {
		return 0
	}
	return it.currentKV.TrancID
}

func (it *TwoMergeIterator) Seek(key string) bool {
	if it.itA != nil {
		it.itA.Seek(key)
	}
	if it.itB != nil {
		it.itB.Seek(key)
	}
	it.UpdateCurrent()
	return it.Valid()
}

func (it *TwoMergeIterator) SeekFirst() bool {
	if it.itA != nil {
		it.itA.SeekFirst()
	}
	if it.itB != nil {
		it.itB.SeekFirst()
	}
	it.UpdateCurrent()
	return it.Valid()
}

func (it *TwoMergeIterator) Next() {
	if it.currentKV == nil {
		return
	}

	if it.chooseA {
		keyA := it.itA.Key()
		it.itA.Next()
		if it.itB != nil && it.itB.Valid() && it.itB.Key() == keyA {
			it.itB.Next()
		}
	} else {
		it.itB.Next()
	}

	it.UpdateCurrent()
}

func (it *TwoMergeIterator) Close() error {
	var err error
	if it.itA != nil {
		if e := it.itA.Close(); e != nil {
			err = e
		}
	}
	if it.itB != nil {
		if e := it.itB.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
