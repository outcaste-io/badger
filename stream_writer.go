/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"encoding/hex"
	"fmt"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/golang/protobuf/proto"
	"github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/badger/v3/table"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

// StreamWriter is used to write data coming from multiple streams. The streams must not have any
// overlapping key ranges. Within each stream, the keys must be sorted. Badger Stream framework is
// capable of generating such an output. So, this StreamWriter can be used at the other end to build
// BadgerDB at a much faster pace by writing SSTables (and value logs) directly to LSM tree levels
// without causing any compactions at all. This is way faster than using batched writer or using
// transactions, but only applicable in situations where the keys are pre-sorted and the DB is being
// bootstrapped. Existing data would get deleted when using this writer. So, this is only useful
// when restoring from backup or replicating DB across servers.
//
// StreamWriter should not be called on in-use DB instances. It is designed only to bootstrap new
// DBs.
type StreamWriter struct {
	writeLock       sync.Mutex
	db              *DB
	done            func()
	throttle        *y.Throttle
	maxVersion      uint64
	writers         map[uint32]*sortedWriter
	prevLevel       int
	senderPrevLevel int
	keyId           map[uint64]*pb.DataKey // map to store stores reader's keyId to data key.
	// Writer might receive tables first, and then receive keys. If true, that means we have
	// started processing keys.
	processingKeys bool
}

// NewStreamWriter creates a StreamWriter. Right after creating StreamWriter, Prepare must be
// called. The memory usage of a StreamWriter is directly proportional to the number of streams
// possible. So, efforts must be made to keep the number of streams low. Stream framework would
// typically use 16 goroutines and hence create 16 streams.
func (db *DB) NewStreamWriter() *StreamWriter {
	return &StreamWriter{
		db: db,
		// throttle shouldn't make much difference. Memory consumption is based on the number of
		// concurrent streams being processed.
		throttle: y.NewThrottle(16),
		writers:  make(map[uint32]*sortedWriter),
		keyId:    make(map[uint64]*pb.DataKey),
	}
}

// Prepare should be called before writing any entry to StreamWriter. It deletes all data present in
// existing DB, stops compactions and any writes being done by other means. Be very careful when
// calling Prepare, because it could result in permanent data loss. Not calling Prepare would result
// in a corrupt Badger instance. Use PrepareIncremental to do incremental stream write.
func (sw *StreamWriter) Prepare() error {
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	done, err := sw.db.dropAll()
	// Ensure that done() is never called more than once.
	var once sync.Once
	sw.done = func() { once.Do(done) }
	return err
}

// PrepareIncremental should be called before writing any entry to StreamWriter incrementally.
// In incremental stream write, the tables are written at one level above the current base level.
func (sw *StreamWriter) PrepareIncremental() error {
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	// Ensure that done() is never called more than once.
	var once sync.Once

	// prepareToDrop will stop all the incoming writes and process any pending flush tasks.
	// Before we start writing, we'll stop the compactions because no one else should be writing to
	// the same level as the stream writer is writing to.
	f, err := sw.db.prepareToDrop()
	if err != nil {
		sw.done = func() { once.Do(f) }
		return err
	}
	sw.db.stopCompactions()
	done := func() {
		sw.db.startCompactions()
		f()
	}
	sw.done = func() { once.Do(done) }

	isEmptyDB := true
	for _, level := range sw.db.Levels() {
		if level.NumTables > 0 {
			sw.prevLevel = level.Level
			isEmptyDB = false
			break
		}
	}
	if isEmptyDB {
		// If DB is empty, we should allow doing incremental stream write.
		return nil
	}
	if sw.prevLevel == 0 {
		return fmt.Errorf("unable to do incremental writes because L0 has data")
	}
	return nil
}

// Write writes KVList to DB. Each KV within the list contains the stream id which StreamWriter
// would use to demux the writes. Write is thread safe and can be called concurrently by multiple
// goroutines.
func (sw *StreamWriter) Write(buf *z.Buffer) error {
	if buf.LenNoPadding() == 0 {
		return nil
	}

	// closedStreams keeps track of all streams which are going to be marked as done. We are
	// keeping track of all streams so that we can close them at the end, after inserting all
	// the valid kvs.
	closedStreams := make(map[uint32]struct{})
	streamReqs := make(map[uint32]*request)

	err := buf.SliceIterate(func(s []byte) error {
		var kv pb.KV
		if err := kv.Unmarshal(s); err != nil {
			return err
		}
		if kv.StreamDone {
			closedStreams[kv.StreamId] = struct{}{}
			return nil
		}

		// Panic if some kv comes after stream has been marked as closed.
		if _, ok := closedStreams[kv.StreamId]; ok {
			panic(fmt.Sprintf("write performed on closed stream: %d", kv.StreamId))
		}
		switch kv.Kind {
		case pb.KV_DATA_KEY:
			y.AssertTrue(len(sw.db.opt.EncryptionKey) > 0)
			var dk pb.DataKey
			if err := proto.Unmarshal(kv.Value, &dk); err != nil {
				return errors.Wrapf(err, "unmarshal failed %s", kv.Value)
			}
			readerId := dk.KeyId
			if _, ok := sw.keyId[readerId]; !ok {
				// Insert the data key to the key registry if not already inserted.
				id, err := sw.db.registry.AddKey(dk)
				if err != nil {
					return errors.Wrap(err, "failed to write data key")
				}
				dk.KeyId = id
				sw.keyId[readerId] = &dk
			}
			return nil
		case pb.KV_FILE:
			// All tables should be recieved before any of the keys.
			if sw.processingKeys {
				return errors.New("Received pb.KV_FILE after pb.KV_KEY")
			}
			var change pb.ManifestChange
			if err := proto.Unmarshal(kv.Key, &change); err != nil {
				return errors.Wrap(err, "unable to unmarshal manifest change")
			}
			level := int(change.Level)
			if sw.senderPrevLevel == 0 {
				// We received the first file, set the sender's and receiver's max levels.
				sw.senderPrevLevel = level
				sw.prevLevel = len(sw.db.lc.levels) - 1
			}

			// This is based on the assumption that the tables from the last
			// level will be sent first and then the second last level tables.
			// As long as the kv.Version (which stores the level) is same as
			// the prevLevel, we know we're processing a last level table.
			// The last level for this DB can be 8 while the DB that's sending
			// this could have the last level at 7.
			if sw.senderPrevLevel != level {
				// If the previous level and the current level is different, we
				// must be processing a table from the next last level.
				sw.senderPrevLevel = level
				sw.prevLevel--
			}
			dk := sw.keyId[change.KeyId]
			return sw.db.lc.AddTable(&kv, sw.prevLevel, dk, &change)
		case pb.KV_KEY:
			// Pass. The following code will handle the keys.
		}

		sw.processingKeys = true
		var meta, userMeta byte
		if len(kv.Meta) > 0 {
			meta = kv.Meta[0]
		}
		if len(kv.UserMeta) > 0 {
			userMeta = kv.UserMeta[0]
		}
		if sw.maxVersion < kv.Version {
			sw.maxVersion = kv.Version
		}
		e := &Entry{
			Key:       y.KeyWithTs(kv.Key, kv.Version),
			Value:     y.Copy(kv.Value),
			UserMeta:  userMeta,
			ExpiresAt: kv.ExpiresAt,
			meta:      meta,
		}
		// If the value can be collocated with the key in LSM tree, we can skip
		// writing the value to value log.
		req := streamReqs[kv.StreamId]
		if req == nil {
			req = &request{}
			streamReqs[kv.StreamId] = req
		}
		req.Entries = append(req.Entries, e)
		return nil
	})
	if err != nil {
		return err
	}

	all := make([]*request, 0, len(streamReqs))
	for _, req := range streamReqs {
		all = append(all, req)
	}

	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	// Moved this piece of code to within the lock.
	if sw.prevLevel == 0 {
		// If prevLevel is 0, that means that we have not written anything yet.
		// So, we can write to the maxLevel. newWriter writes to prevLevel - 1,
		// so we can set prevLevel to len(levels).
		sw.prevLevel = len(sw.db.lc.levels)
	}

	for streamID, req := range streamReqs {
		writer, ok := sw.writers[streamID]
		if !ok {
			var err error
			writer, err = sw.newWriter(streamID)
			if err != nil {
				return y.Wrapf(err, "failed to create writer with ID %d", streamID)
			}
			sw.writers[streamID] = writer
		}

		if writer == nil {
			panic(fmt.Sprintf("write performed on closed stream: %d", streamID))
		}

		writer.reqCh <- req
	}

	// Now we can close any streams if required. We will make writer for
	// the closed streams as nil.
	for streamId := range closedStreams {
		writer, ok := sw.writers[streamId]
		if !ok {
			sw.db.opt.Logger.Warningf("Trying to close stream: %d, but no sorted "+
				"writer found for it", streamId)
			continue
		}

		writer.closer.SignalAndWait()
		if err := writer.Done(); err != nil {
			return err
		}

		sw.writers[streamId] = nil
	}
	return nil
}

// Flush is called once we are done writing all the entries. It syncs DB directories. It also
// updates Oracle with maxVersion found in all entries (if DB is not managed).
func (sw *StreamWriter) Flush() error {
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	defer sw.done()

	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.SignalAndWait()
		}
	}

	for _, writer := range sw.writers {
		if writer == nil {
			continue
		}
		if err := writer.Done(); err != nil {
			return err
		}
	}

	if !sw.db.opt.managedTxns {
		if sw.db.orc != nil {
			sw.db.orc.Stop()
		}
		sw.db.orc = newOracle(sw.db.opt)
		sw.db.orc.nextTxnTs = sw.maxVersion
		sw.db.orc.txnMark.Done(sw.maxVersion)
		sw.db.orc.readMark.Done(sw.maxVersion)
		sw.db.orc.incrementNextTs()
	}

	// Wait for all files to be written.
	if err := sw.throttle.Finish(); err != nil {
		return err
	}

	// Sort tables at the end.
	for _, l := range sw.db.lc.levels {
		l.sortTables()
	}

	// Now sync the directories, so all the files are registered.
	if sw.db.opt.ValueDir != sw.db.opt.Dir {
		if err := sw.db.syncDir(sw.db.opt.ValueDir); err != nil {
			return err
		}
	}
	if err := sw.db.syncDir(sw.db.opt.Dir); err != nil {
		return err
	}
	return sw.db.lc.validate()
}

// Cancel signals all goroutines to exit. Calling defer sw.Cancel() immediately after creating a new StreamWriter
// ensures that writes are unblocked even upon early return. Note that dropAll() is not called here, so any
// partially written data will not be erased until a new StreamWriter is initialized.
func (sw *StreamWriter) Cancel() {
	sw.writeLock.Lock()
	defer sw.writeLock.Unlock()

	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Signal()
		}
	}
	for _, writer := range sw.writers {
		if writer != nil {
			writer.closer.Wait()
		}
	}

	if err := sw.throttle.Finish(); err != nil {
		sw.db.opt.Errorf("error in throttle.Finish: %+v", err)
	}

	// Handle Cancel() being called before Prepare().
	if sw.done != nil {
		sw.done()
	}
}

type sortedWriter struct {
	db       *DB
	throttle *y.Throttle
	opts     table.Options

	builder  *table.Builder
	lastKey  []byte
	level    int
	streamID uint32
	reqCh    chan *request
	// Have separate closer for each writer, as it can be closed at any time.
	closer *z.Closer
}

func (sw *StreamWriter) newWriter(streamID uint32) (*sortedWriter, error) {
	bopts := buildTableOptions(sw.db)
	for i := 2; i < sw.db.opt.MaxLevels; i++ {
		bopts.TableSize *= uint64(sw.db.opt.TableSizeMultiplier)
	}
	w := &sortedWriter{
		db:       sw.db,
		opts:     bopts,
		streamID: streamID,
		throttle: sw.throttle,
		builder:  table.NewTableBuilder(bopts),
		reqCh:    make(chan *request, 3),
		closer:   z.NewCloser(1),
		level:    sw.prevLevel - 1, // Write at the level just above the one we were writing to.
	}

	go w.handleRequests()
	return w, nil
}

func (w *sortedWriter) handleRequests() {
	defer w.closer.Done()

	process := func(req *request) {
		for _, e := range req.Entries {
			// If badger is running in InMemory mode, len(req.Ptrs) == 0.
			// Sorted stream writer receives Key-Value (not a pointer to value). So, its upto the
			// writer (and not the sender) to determine if the Value goes to vlog or stays in SST
			// only. In managed mode, we do not write values to vlog and hence we would not have
			// req.Ptrs initialized.
			vs := y.ValueStruct{
				Value:     e.Value,
				Meta:      e.meta,
				UserMeta:  e.UserMeta,
				ExpiresAt: e.ExpiresAt,
			}
			if err := w.Add(e.Key, vs); err != nil {
				panic(err)
			}
		}
	}

	for {
		select {
		case req := <-w.reqCh:
			process(req)
		case <-w.closer.HasBeenClosed():
			close(w.reqCh)
			for req := range w.reqCh {
				process(req)
			}
			return
		}
	}
}

// Add adds key and vs to sortedWriter.
func (w *sortedWriter) Add(key []byte, vs y.ValueStruct) error {
	if len(w.lastKey) > 0 && y.CompareKeys(key, w.lastKey) <= 0 {
		return errors.Errorf("keys not in sorted order (last key: %s, key: %s)",
			hex.Dump(w.lastKey), hex.Dump(key))
	}

	sameKey := y.SameKey(key, w.lastKey)

	// Same keys should go into the same SSTable.
	if !sameKey && w.builder.ReachedCapacity() {
		if err := w.send(false); err != nil {
			return err
		}
	}

	w.lastKey = y.SafeCopy(w.lastKey, key)
	w.builder.Add(key, vs)
	return nil
}

func (w *sortedWriter) send(done bool) error {
	if err := w.throttle.Do(); err != nil {
		return err
	}
	go func(builder *table.Builder) {
		err := w.createTable(builder)
		w.throttle.Done(err)
	}(w.builder)
	// If done is true, this indicates we can close the writer.
	// No need to allocate underlying TableBuilder now.
	if done {
		w.builder = nil
		return nil
	}

	w.builder = table.NewTableBuilder(w.opts)
	return nil
}

// Done is called once we are done writing all keys and valueStructs
// to sortedWriter. It completes writing current SST to disk.
func (w *sortedWriter) Done() error {
	if w.builder.Empty() {
		w.builder.Close()
		// Assign builder as nil, so that underlying memory can be garbage collected.
		w.builder = nil
		return nil
	}

	return w.send(true)
}

func (w *sortedWriter) createTable(builder *table.Builder) error {
	defer builder.Close()
	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileID := w.db.lc.reserveFileID()
	var tbl *table.Table
	if w.db.opt.InMemory {
		data := builder.Finish()
		var err error
		if tbl, err = table.OpenInMemoryTable(data, fileID, builder.Opts()); err != nil {
			return err
		}
	} else {
		var err error
		fname := table.NewFilename(fileID, w.db.opt.Dir)
		if tbl, err = table.CreateTable(fname, builder); err != nil {
			return err
		}
	}
	lc := w.db.lc

	lhandler := lc.levels[w.level]
	// Now that table can be opened successfully, let's add this to the MANIFEST.
	change := &pb.ManifestChange{
		Id:          tbl.ID(),
		KeyId:       tbl.KeyID(),
		Op:          pb.ManifestChange_CREATE,
		Level:       uint32(lhandler.level),
		Compression: uint32(tbl.CompressionType()),
	}
	if err := w.db.manifest.addChanges([]*pb.ManifestChange{change}); err != nil {
		return err
	}

	// We are not calling lhandler.replaceTables() here, as it sorts tables on every addition.
	// We can sort all tables only once during Flush() call.
	lhandler.addTable(tbl)

	// Release the ref held by OpenTable.
	_ = tbl.DecrRef()
	w.db.opt.Infof("Table created: %d at level: %d for stream: %d. Size: %s\n",
		fileID, lhandler.level, w.streamID, humanize.IBytes(uint64(tbl.Size())))
	return nil
}
