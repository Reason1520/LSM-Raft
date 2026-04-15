package lsm

func (lsme *LSMEngine) ScanFrom(trancID uint64, start string) Iterator {
	memIt := lsme.memtable.NewMemtableIterator(true, trancID)
	levelIt := NewLevelIterator(lsme, trancID)
	it := NewTwoMergeIterator(memIt, levelIt, trancID)
	if start == "" {
		if !it.SeekFirst() {
			it.Close()
			return nil
		}
		return it
	}
	if !it.Seek(start) {
		it.Close()
		return nil
	}
	return it
}
