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

import "sync/atomic"

// NewWriteBatchAt is similar to NewWriteBatch but it allows user to set the commit timestamp.
// NewWriteBatchAt is supposed to be used only in the managed mode.
func (db *DB) NewWriteBatchAt(commitTs uint64) *WriteBatch {
	wb := db.newWriteBatch(true)
	wb.commitTs = commitTs
	wb.txn.commitTs = commitTs
	return wb
}
func (db *DB) NewManagedWriteBatch() *WriteBatch {
	wb := db.newWriteBatch(true)
	return wb
}

// CommitAt commits the transaction, following the same logic as Commit(), but
// at the given commit timestamp. This will panic if not used with managed transactions.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func (txn *Txn) CommitAt(commitTs uint64, callback func(error)) error {
	txn.commitTs = commitTs
	if callback == nil {
		return txn.Commit()
	}
	txn.CommitWith(callback)
	return nil
}

// SetDiscardTs sets a timestamp at or below which, any invalid or deleted
// versions can be discarded from the LSM tree, and thence from the value log to
// reclaim disk space. Can only be used with managed transactions.
func (db *DB) SetDiscardTs(ts uint64) {
	atomic.StoreUint64(&db.discardTs, ts)
}
