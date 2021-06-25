package badgerdb

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/alash3al/goukv"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

// Provider represents a provider
type Provider struct {
	l  *sync.RWMutex
	db *badger.DB
}

// Open implements goukv.Open
func (p Provider) Open(dsn *goukv.DSN) (goukv.Provider, error) {
	path := dsn.Hostname() + dsn.Path()

	syncWrites := dsn.GetBool("sync_writes")

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
		l:  &sync.RWMutex{},
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

func (p Provider) Incr(k []byte, delta float64) (float64, error) {
	p.l.Lock()
	defer p.l.Unlock()

	ttl, err := p.TTL(k)
	if err != goukv.ErrKeyNotFound && err != nil {
		return 0, err
	}

	val, err := p.Get(k)
	if err != goukv.ErrKeyNotFound && err != nil {
		return 0, err
	}

	var valAsFloat float64

	if val == nil {
		valAsFloat = 0
	} else {
		parsedVal, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, err
		}
		valAsFloat = parsedVal
	}

	valAsFloat += delta

	var newttl time.Duration
	if ttl != nil {
		newttl = time.Until(*ttl)
	}

	return valAsFloat, p.Put(&goukv.Entry{
		Key:   k,
		Value: []byte(fmt.Sprintf("%f", valAsFloat)),
		TTL:   newttl,
	})
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

// TTL implements goukv.TTL
func (p Provider) TTL(k []byte) (*time.Time, error) {
	var t *time.Time
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return goukv.ErrKeyNotFound
		}

		if err != nil {
			return err
		}

		expiresAt := item.ExpiresAt()
		if expiresAt > 0 {
			toUnix := time.Unix(int64(expiresAt), 0)
			t = &toUnix
		}

		return err
	})

	return t, err
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
func (p Provider) Scan(opts goukv.ScanOpts) error {
	if opts.Scanner == nil {
		return nil
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

	return nil
}
