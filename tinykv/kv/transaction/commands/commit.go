package commands

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Commit struct {
	CommandBase
	request *kvrpcpb.CommitRequest
}

func NewCommit(request *kvrpcpb.CommitRequest) Commit {
	return Commit{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (c *Commit) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	commitTs := c.request.CommitVersion
	// Check if the commitTs is invalid, the commitTs must be greater than the transaction startTs. If not
	// report unexpected error.
	if commitTs <= txn.StartTS {
		panic("Commit.PrepareWrites: time went backwards")
	}
	response := new(kvrpcpb.CommitResponse)

	// Commit each key.
	for _, k := range c.request.Keys {
		resp, e := commitKey(k, commitTs, txn, response)
		if resp != nil || e != nil {
			return response, e
		}
	}

	return response, nil
}

func commitKey(key []byte, commitTs uint64, txn *mvcc.MvccTxn, response interface{}) (interface{}, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	// If there is no correspond lock for this transaction.
	log.Debug("commitKey", zap.Uint64("startTS", txn.StartTS),
		zap.Uint64("commitTs", commitTs),
		zap.String("key", hex.EncodeToString(key)))
	if lock == nil || lock.Ts != txn.StartTS {
		// Key is locked by a different transaction, or there is no lock on the key. It's needed to
		// check the commit/rollback record for this key, if nothing is found report lock not found
		// error. Also the commit request could be stale that it's already committed or rolled back.
		respValue := reflect.ValueOf(response)
		var keyError *kvrpcpb.KeyError

		write, conflictTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.StartTS == txn.StartTS {
			// if write record by current txn found
			if write.Kind == mvcc.WriteKindRollback {
				keyError = &kvrpcpb.KeyError{Abort: fmt.Sprintf("already aborted %v", key)}
			} else {
				// already committed, stale commit request
				return nil, nil
			}
		}
		if lock == nil {
			keyError = &kvrpcpb.KeyError{Retryable: fmt.Sprintf("lock not found for key %v", key)}
		} else {
			keyError = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,
				ConflictTs: conflictTs,
				Key:        key,
				Primary:    lock.Primary,
			}}
		}
		reflect.Indirect(respValue).FieldByName("Error").Set(reflect.ValueOf(keyError))
		return response, nil
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (c *Commit) WillWrite() [][]byte {
	return c.request.Keys
}
