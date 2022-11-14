package badger

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/outcaste-io/badger/v4/y"
	"github.com/outcaste-io/ristretto/z"
)

type LifetimeStats struct {
	sync.RWMutex
	mf *z.MmapFile
}

var (
	maxSz            int = 1 << 20
	idxVersion       int = 0
	idxOpen          int = 1
	idxClose         int = 2
	idxNumWrites     int = 3
	idxReservedBelow int = 128
)

func InitLifetimeStats(path string) *LifetimeStats {
	mf, err := z.OpenMmapFile(path, os.O_RDWR|os.O_CREATE, maxSz)
	if err == z.NewFile {
		for i := range mf.Data {
			mf.Data[i] = 0x0
		}
		y.Check(mf.Sync())
		err = nil
	}
	y.Check(err)

	lf := &LifetimeStats{mf: mf}
	lf.UpdateAt(idxOpen, 1)
	return lf
}

func getOffset(idx int) int {
	off := idx * 8
	y.AssertTrue(off < maxSz)
	return off
}

func (lf *LifetimeStats) readAt(idx int) uint64 {
	offset := getOffset(idx)
	return binary.BigEndian.Uint64(lf.mf.Data[offset : offset+8])
}
func (lf *LifetimeStats) updateAt(idx int, delta uint64) {
	val := lf.readAt(idx)
	val += delta

	off := getOffset(idx)
	binary.BigEndian.PutUint64(lf.mf.Data[off:off+8], val)
}
func (lf *LifetimeStats) ReadAt(idx int) uint64 {
	lf.RLock()
	val := lf.readAt(idx)
	lf.RUnlock()
	return val
}

func (lf *LifetimeStats) UpdateAt(idx int, delta uint64) {
	lf.Lock()
	defer lf.Unlock()

	y.AssertTrue(idx > idxReservedBelow)
	lf.updateAt(idx, delta)
	lf.updateAt(idxNumWrites, 1)
}

func (lf *LifetimeStats) Close() error {
	lf.UpdateAt(idxClose, 1)
	return lf.mf.Close(0)
}

func (lf *LifetimeStats) Stats() map[int]uint64 {
	res := make(map[int]uint64)

	lf.RLock()
	for i := 0; i < (1<<20)/8; i++ {
		if val := lf.readAt(i); val > 0 {
			res[i] = val
		}
	}
	lf.RUnlock()

	return res
}
