package badgerdb

import (
	"errors"
	"time"

	"github.com/alash3al/goukv"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

// Provider represents a provider
type Provider struct {
	db *badger.DB
}

// Open implements goukv.Open
func (p Provider) Open(opts map[string]interface{}) (goukv.Provider, error) {
	path, ok := opts["path"].(string)
	if !ok {
		return nil, errors.New("must specify path")
	}

	syncWrites, ok := opts["sync_writes"].(bool)
	if !ok {
		syncWrites = false
	}

	badgerOpts := badger.DefaultOptions(path)

	badgerOpts.WithSyncWrites(syncWrites)
	badgerOpts.WithLogger(nil)
	badgerOpts.WithKeepL0InMemory(true)
	badgerOpts.WithCompression(options.Snappy)

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	go (func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			for {
				err := db.RunValueLogGC(0.5)
				if err != nil {
					break
				}
			}
		}
	})()

	return &Provider{
		db: db,
	}, nil
}

// Put implements goukv.Put
func (p Provider) Put(entry *goukv.Entry) error {
	return p.db.Update(func(txn *badger.Txn) error {
		if entry.TTL > 0 {
			badgerEntry := badger.NewEntry(entry.Key, entry.Value)
			badgerEntry.WithTTL(entry.TTL)
			return txn.SetEntry(badgerEntry)
		}

		return txn.Set(entry.Key, entry.Value)
	})
}

// Batch perform multi put operation, empty value means *delete*
func (p Provider) Batch(entries []*goukv.Entry) error {
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

	for _, entry := range entries {
		var err error
		if entry.Value == nil {
			err = batch.Delete(entry.Key)
		} else {
			if entry.TTL > 0 {
				badgerEntry := badger.NewEntry(entry.Key, entry.Value)
				badgerEntry.WithTTL(entry.TTL)

				err = batch.SetEntry(badgerEntry)
			} else {
				err = batch.Set(entry.Key, entry.Value)
			}
		}

		if err != nil {
			batch.Cancel()
			return err
		}
	}

	return batch.Flush()
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	var data []byte
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return goukv.ErrKeyNotFound
		}

		if err != nil {
			return err
		}

		d, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		data = d

		return err
	})

	return data, err
}

// Delete implements goukv.Delete
func (p Provider) Delete(k []byte) error {
	return p.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

// Close implements goukv.Close
func (p Provider) Close() error {
	return p.db.Close()
}

// Scan implements goukv.Scan
func (p Provider) Scan(opts goukv.ScanOpts) {
	if opts.Scanner == nil {
		return
	}

	txn := p.db.NewTransaction(false)
	defer txn.Commit()

	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Reverse = opts.ReverseScan

	if len(opts.Prefix) > 0 {
		iterOpts.Prefix = opts.Prefix
	}

	iter := txn.NewIterator(iterOpts)
	defer iter.Close()

	if opts.Offset != nil {
		iter.Seek(opts.Offset)
	} else {
		iter.Rewind()
	}

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()

		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			break
		}

		if !opts.Scanner(key, val) {
			break
		}
	}
}
