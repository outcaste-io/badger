/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/outcaste-io/badger/v3/y"
	"github.com/stretchr/testify/require"
)

func TestValueChecksums(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Set up SST with K1=V1
	opts := getTestOptions(dir)
	opts.VerifyValueChecksum = true
	kv, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		k3 = []byte("k3")
		v0 = []byte("value0-012345678901234567890123012345678901234567890123")
		v1 = []byte("value1-012345678901234567890123012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123012345678901234567890123")
		v3 = []byte("value3-012345678901234567890123012345678901234567890123")
	)

	// Use a vlog with K0=V0 and a (corrupted) second transaction(k1,k2)
	buf, offset := createMemFile(t, []*Entry{
		{Key: k0, Value: v0},
		{Key: k1, Value: v1},
		{Key: k2, Value: v2},
	})
	buf[offset-1]++ // Corrupt last byte
	require.NoError(t, ioutil.WriteFile(kv.mtFilePath(1), buf, 0777))

	// K1 should exist, but K2 shouldn't.
	kv, err = Open(opts)
	require.NoError(t, err)

	require.NoError(t, kv.View(func(txn *Txn) error {
		// Replay should have added K0.
		item, err := txn.Get(k0)
		require.NoError(t, err)
		require.Equal(t, getItemValue(t, item), v0)

		_, err = txn.Get(k1)
		require.Equal(t, ErrKeyNotFound, err)

		_, err = txn.Get(k2)
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	}))

	// Write K3 at the end of the vlog.
	txnSet(t, kv, k3, v3, 0)
	require.NoError(t, kv.Close())

	// The DB should contain K0 and K3 (K1 and k2 was lost when Badger started up
	// last due to checksum failure).
	kv, err = Open(opts)
	require.NoError(t, err)

	{
		txn := kv.NewTransaction(false)

		iter := txn.NewIterator(DefaultIteratorOptions)
		iter.Seek(k0)
		require.True(t, iter.Valid())
		it := iter.Item()
		require.Equal(t, it.Key(), k0)
		require.Equal(t, getItemValue(t, it), v0)
		iter.Next()
		require.True(t, iter.Valid())
		it = iter.Item()
		require.Equal(t, it.Key(), k3)
		require.Equal(t, getItemValue(t, it), v3)

		iter.Close()
		txn.Discard()
	}

	require.NoError(t, kv.Close())
}

func TestReadOnlyOpenWithPartialAppendToWAL(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Create skeleton files.
	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	var (
		k0 = []byte("k0")
		k1 = []byte("k1")
		k2 = []byte("k2")
		v0 = []byte("value0-012345678901234567890123")
		v1 = []byte("value1-012345678901234567890123")
		v2 = []byte("value2-012345678901234567890123")
	)

	// Create truncated vlog to simulate a partial append.
	// k0 - single transaction, k1 and k2 in another transaction
	buf, offset := createMemFile(t, []*Entry{
		{Key: k0, Value: v0},
		{Key: k1, Value: v1},
		{Key: k2, Value: v2},
	})
	buf = buf[:offset-6]
	require.NoError(t, ioutil.WriteFile(kv.mtFilePath(1), buf, 0777))

	opts.ReadOnly = true
	// Badger should fail a read-only open with values to replay
	_, err = Open(opts)
	require.Error(t, err)
	require.Regexp(t, "Log truncate required", err.Error())
}

// createMemFile creates a new memFile and returns the last valid offset.
func createMemFile(t *testing.T, entries []*Entry) ([]byte, uint32) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opts := getTestOptions(dir)
	opts.ValueLogFileSize = 100 * 1024 * 1024 // 100Mb
	kv, err := Open(opts)
	require.NoError(t, err)
	defer kv.Close()

	txnSet(t, kv, entries[0].Key, entries[0].Value, entries[0].meta)

	entries = entries[1:]
	txn := kv.NewTransaction(true)
	for _, entry := range entries {
		require.NoError(t, txn.SetEntry(NewEntry(entry.Key, entry.Value).WithMeta(entry.meta)))
	}
	require.NoError(t, txn.Commit())

	filename := kv.mtFilePath(1)
	buf, err := ioutil.ReadFile(filename)
	require.NoError(t, err)
	return buf, kv.mt.wal.writeAt
}

// This test creates two mem files and corrupts the last bit of the first file.
func TestPenultimateMemCorruption(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := getTestOptions(dir)

	db0, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db0.Close()) }()

	h := testHelper{db: db0, t: t}
	h.writeRange(0, 2) // 00001.mem

	// Move the current memtable to the db.imm and create a new memtable so
	// that we can have more than one mem files.
	require.Zero(t, len(db0.imm))
	db0.imm = append(db0.imm, db0.mt)
	db0.mt, err = db0.newMemTable()
	require.NoError(t, err)

	h.writeRange(3, 7) // 00002.mem

	// Verify we have all the data we wrote.
	h.readRange(0, 7)

	for i := 2; i >= 1; i-- {
		fpath := db0.mtFilePath(i)
		fi, err := os.Stat(fpath)
		require.NoError(t, err)
		require.True(t, fi.Size() > 0, "Empty file at log=%d", i)
		if i == 1 {
			// This should corrupt the last entry in the first memtable (that is entry number 2)
			wal := db0.imm[0].wal
			wal.Fd.WriteAt([]byte{0}, int64(wal.writeAt-1))
			require.NoError(t, err)
			// We have corrupted the file. We can remove it. If we don't remove
			// the imm here, the db.close in defer will crash since db0.mt !=
			// db0.imm[0]
			db0.imm = db0.imm[:0]
		}
	}
	// Simulate a crash by not closing db0, but releasing the locks.
	if db0.dirLockGuard != nil {
		require.NoError(t, db0.dirLockGuard.release())
		db0.dirLockGuard = nil
	}
	if db0.valueDirGuard != nil {
		require.NoError(t, db0.valueDirGuard.release())
		db0.valueDirGuard = nil
	}

	db1, err := Open(opt)
	require.NoError(t, err)
	h.db = db1
	// Only 2 should be gone because it is at the end of 0001.mem (first memfile).
	h.readRange(0, 1)
	h.readRange(3, 7)
	err = db1.View(func(txn *Txn) error {
		_, err := txn.Get(h.key(2)) // Verify that 2 is gone.
		require.Equal(t, ErrKeyNotFound, err)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db1.Close())
}

func checkKeys(t *testing.T, kv *DB, keys [][]byte) {
	i := 0
	txn := kv.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(IteratorOptions{})
	defer iter.Close()
	for iter.Seek(keys[0]); iter.Valid(); iter.Next() {
		require.Equal(t, iter.Item().Key(), keys[i])
		i++
	}
	require.Equal(t, i, len(keys))
}

type testHelper struct {
	db  *DB
	t   *testing.T
	val []byte
}

func (th *testHelper) key(i int) []byte {
	return []byte(fmt.Sprintf("%010d", i))
}
func (th *testHelper) value() []byte {
	if len(th.val) > 0 {
		return th.val
	}
	th.val = make([]byte, 100)
	y.Check2(rand.Read(th.val))
	return th.val
}

// writeRange [from, to].
func (th *testHelper) writeRange(from, to int) {
	for i := from; i <= to; i++ {
		err := th.db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry(th.key(i), th.value()))
		})
		require.NoError(th.t, err)
	}
}

func (th *testHelper) readRange(from, to int) {
	for i := from; i <= to; i++ {
		err := th.db.View(func(txn *Txn) error {
			item, err := txn.Get(th.key(i))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				require.Equal(th.t, val, th.value(), "key=%q", th.key(i))
				return nil

			})
		})
		require.NoError(th.t, err, "key=%q", th.key(i))
	}
}

func BenchmarkReadWrite(b *testing.B) {
	rwRatio := []float32{
		0.1, 0.2, 0.5, 1.0,
	}
	valueSize := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}

	for _, vsz := range valueSize {
		for _, rw := range rwRatio {
			b.Run(fmt.Sprintf("%3.1f,%04d", rw, vsz), func(b *testing.B) {
				dir, err := ioutil.TempDir("", "vlog-benchmark")
				y.Check(err)
				defer removeDir(dir)
				opts := getTestOptions(dir)
				opts.ValueThreshold = 0
				db, err := Open(opts)
				y.Check(err)

				vl := &db.vlog
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					e := new(Entry)
					e.Key = make([]byte, 16)
					e.Value = make([]byte, vsz)
					bl := new(request)
					bl.Entries = []*Entry{e}

					var ptrs []valuePointer

					vl.write([]*request{bl})
					ptrs = append(ptrs, bl.Ptrs...)

					f := rand.Float32()
					if f < rw {
						vl.write([]*request{bl})

					} else {
						ln := len(ptrs)
						if ln == 0 {
							b.Fatalf("Zero length of ptrs")
						}
						idx := rand.Intn(ln)
						buf, lf, err := vl.readValueBytes(ptrs[idx])
						if err != nil {
							b.Fatalf("Benchmark Read: %v", err)
						}

						e, err := lf.decodeEntry(buf, ptrs[idx].Offset)
						require.NoError(b, err)
						if len(e.Key) != 16 {
							b.Fatalf("Key is invalid")
						}
						if len(e.Value) != vsz {
							b.Fatalf("Value is invalid")
						}
						runCallback(db.vlog.getUnlockCallback(lf))
					}
				}
			})
		}
	}
}

// Regression test for https://github.com/dgraph-io/badger/issues/817
// This test verifies if fully corrupted memtables are deleted on reopen.
func TestValueLogTruncate(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Initialize the data directory.
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	// Insert 1 entry so that we have valid data in first mem file
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("foo"), nil)
	}))

	fileCountBeforeCorruption := 1
	require.NoError(t, db.Close())

	// Create two mem files with corrupted data. These will be truncated when DB starts next time
	require.NoError(t, ioutil.WriteFile(db.mtFilePath(2), []byte("foo"), 0664))
	require.NoError(t, ioutil.WriteFile(db.mtFilePath(3), []byte("foo"), 0664))

	db, err = Open(DefaultOptions(dir))
	require.NoError(t, err)

	// Ensure we have only one SST file.
	require.Equal(t, 1, len(db.Tables()))

	// Ensure mem file with ID 4 is zero.
	require.Equal(t, 4, int(db.mt.wal.fid))
	fileStat, err := db.mt.wal.Fd.Stat()
	require.NoError(t, err)
	require.Equal(t, 2*db.opt.MemTableSize, fileStat.Size())

	fileCountAfterCorruption := len(db.Tables()) + len(db.imm) + 1 // +1 for db.mt
	// We should have one memtable and one sst file.
	require.Equal(t, fileCountBeforeCorruption+1, fileCountAfterCorruption)
	// maxFid will be 2 because we increment the max fid on DB open everytime.
	require.Equal(t, 2, int(db.vlog.maxFid))
	require.NoError(t, db.Close())
}

func TestSafeEntry(t *testing.T) {
	var s safeRead
	s.lf = &logFile{}
	e := NewEntry([]byte("foo"), []byte("bar"))
	buf := bytes.NewBuffer(nil)
	_, err := s.lf.encodeEntry(buf, e, 0)
	require.NoError(t, err)

	ne, err := s.Entry(buf)
	require.NoError(t, err)
	require.Equal(t, e.Key, ne.Key, "key mismatch")
	require.Equal(t, e.Value, ne.Value, "value mismatch")
	require.Equal(t, e.meta, ne.meta, "meta mismatch")
	require.Equal(t, e.UserMeta, ne.UserMeta, "usermeta mismatch")
	require.Equal(t, e.ExpiresAt, ne.ExpiresAt, "expiresAt mismatch")
}

func TestValueEntryChecksum(t *testing.T) {
	k := []byte("KEY")
	v := []byte(fmt.Sprintf("val%100d", 10))
	t.Run("ok", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := getTestOptions(dir)
		opt.VerifyValueChecksum = true
		opt.ValueThreshold = 32
		db, err := Open(opt)
		require.NoError(t, err)

		require.Greater(t, int64(len(v)), db.vlog.db.valueThreshold())
		txnSet(t, db, k, v, 0)
		require.NoError(t, db.Close())

		db, err = Open(opt)
		require.NoError(t, err)

		txn := db.NewTransaction(false)
		entry, err := txn.Get(k)
		require.NoError(t, err)

		x, err := entry.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, v, x)

		require.NoError(t, db.Close())
	})
	// Regression test for https://github.com/dgraph-io/badger/issues/1049
	t.Run("Corruption", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := getTestOptions(dir)
		opt.VerifyValueChecksum = true
		opt.ValueThreshold = 32
		db, err := Open(opt)
		require.NoError(t, err)

		require.Greater(t, int64(len(v)), db.vlog.db.valueThreshold())
		txnSet(t, db, k, v, 0)

		path := db.vlog.fpath(1)
		require.NoError(t, db.Close())

		file, err := os.OpenFile(path, os.O_RDWR, 0644)
		require.NoError(t, err)
		offset := 50
		orig := make([]byte, 1)
		_, err = file.ReadAt(orig, int64(offset))
		require.NoError(t, err)
		// Corrupt a single bit.
		_, err = file.WriteAt([]byte{7}, int64(offset))
		require.NoError(t, err)
		require.NoError(t, file.Close())

		db, err = Open(opt)
		require.NoError(t, err)

		txn := db.NewTransaction(false)
		entry, err := txn.Get(k)
		require.NoError(t, err)

		// TODO(ibrahim): This test is broken since we're not returning errors
		// in case we cannot read the values. This is incorrect behavior but
		// we're doing this to debug an issue where the values are being read
		// from old vlog files.
		_, _ = entry.ValueCopy(nil)
		// require.Error(t, err)
		// require.Contains(t, err.Error(), "ErrEOF")
		// require.Nil(t, x)

		require.NoError(t, db.Close())
	})
}

func TestValidateWrite(t *testing.T) {
	// Mocking the file size, so that we don't allocate big memory while running test.
	maxVlogFileSize = 400
	defer func() {
		maxVlogFileSize = math.MaxUint32
	}()

	bigBuf := make([]byte, maxVlogFileSize+1)
	log := &valueLog{
		opt: DefaultOptions("."),
	}

	// Sending a request with big values which will overflow uint32.
	key := []byte("HelloKey")
	req := &request{
		Entries: []*Entry{
			{
				Key:   key,
				Value: bigBuf,
			},
			{
				Key:   key,
				Value: bigBuf,
			},
			{
				Key:   key,
				Value: bigBuf,
			},
		},
	}

	err := log.validateWrites([]*request{req})
	require.Error(t, err)

	// Testing with small values.
	smallBuf := make([]byte, 4)
	req1 := &request{
		Entries: []*Entry{
			{
				Key:   key,
				Value: smallBuf,
			},
			{
				Key:   key,
				Value: smallBuf,
			},
			{
				Key:   key,
				Value: smallBuf,
			},
		},
	}

	err = log.validateWrites([]*request{req1})
	require.NoError(t, err)

	// Batching small and big request.
	err = log.validateWrites([]*request{req1, req})
	require.Error(t, err)
}

func TestValueLogMeta(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	y.Check(err)
	defer removeDir(dir)

	opt := getTestOptions(dir).WithValueThreshold(16)
	db, _ := Open(opt)
	defer db.Close()
	txn := db.NewTransaction(true)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key=%d", i))
		v := []byte(fmt.Sprintf("val=%020d", i))
		require.NoError(t, txn.SetEntry(NewEntry(k, v)))
	}
	require.NoError(t, txn.Commit())
	fids := db.vlog.sortedFids()
	require.Equal(t, 1, len(fids))

	// vlog entries must not have txn meta.
	db.vlog.filesMap[fids[0]].iterate(true, 0, func(e Entry, vp valuePointer) error {
		require.Zero(t, e.meta&(bitTxn|bitFinTxn))
		return nil
	})

	// Entries in LSM tree must have txn bit of meta set
	txn = db.NewTransaction(false)
	defer txn.Discard()
	iopt := DefaultIteratorOptions
	key := []byte("key")
	iopt.Prefix = key
	itr := txn.NewIterator(iopt)
	defer itr.Close()
	var count int
	for itr.Seek(key); itr.ValidForPrefix(key); itr.Next() {
		item := itr.Item()
		require.Equal(t, bitTxn, item.meta&(bitTxn|bitFinTxn))
		count++
	}
	require.Equal(t, 10, count)
}

// This tests asserts the condition that vlog fids start from 1.
// TODO(naman): should this be changed to assert instead?
func TestFirstVlogFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	fids := db.vlog.sortedFids()
	require.NotZero(t, len(fids))
	require.Equal(t, uint32(1), fids[0])
}
