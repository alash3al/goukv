package leveldb

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/alash3al/goukv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Provider represents a driver
type Provider struct {
	db         *leveldb.DB
	l          *sync.RWMutex
	syncWrites bool
}

// Open implements goukv.Open
func (p Provider) Open(dsn *goukv.DSN) (goukv.Provider, error) {
	path := dsn.Hostname() + dsn.Path()
	syncWrites := dsn.GetBool("sync_writes")

	o := &opt.Options{
		Filter:         filter.NewBloomFilter(10),
		ErrorIfMissing: false,
		Compression:    9,
		NoSync:         syncWrites,
	}

	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		return nil, err
	}

	return &Provider{
		db:         db,
		l:          &sync.RWMutex{},
		syncWrites: syncWrites,
	}, nil
}

// Put implements goukv.Put
func (p Provider) Put(e *goukv.Entry) error {
	return p.db.Put(e.Key, EntryToValue(e).Bytes(), &opt.WriteOptions{
		Sync: p.syncWrites,
	})
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

// Batch perform multi put operation, empty value means *delete*
func (p Provider) Batch(entries []*goukv.Entry) error {
	batch := new(leveldb.Batch)

	for _, entry := range entries {
		if entry.Value == nil {
			batch.Delete(entry.Key)
		} else {
			batch.Put(entry.Key, EntryToValue(entry).Bytes())
		}
	}

	return p.db.Write(batch, &opt.WriteOptions{
		Sync: p.syncWrites,
	})
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	b, err := p.db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	val := BytesToValue(b)

	if val.Expires != nil && val.IsExpired() {
		return nil, goukv.ErrKeyExpired
	}

	return val.Value, err
}

// TTL implements goukv.TTL
func (p Provider) TTL(k []byte) (*time.Time, error) {
	b, err := p.db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, goukv.ErrKeyNotFound
	}

	if err != nil {
		return nil, err
	}

	val := BytesToValue(b)

	return val.Expires, nil
}

// Delete implements goukv.Delete
func (p Provider) Delete(k []byte) error {
	return p.db.Delete(k, &opt.WriteOptions{
		Sync: p.syncWrites,
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

	var iter iterator.Iterator
	var next func() bool

	if opts.Prefix != nil {
		iter = p.db.NewIterator(util.BytesPrefix(opts.Prefix), nil)
	} else {
		iter = p.db.NewIterator(nil, nil)
	}

	if opts.ReverseScan {
		next = iter.Prev
	} else {
		next = iter.Next
	}

	if opts.Offset != nil {
		iter.Seek(opts.Offset)
	}

	if opts.ReverseScan && opts.Offset == nil && opts.Prefix == nil {
		iter.Last()
	}

	if opts.Offset != nil && !opts.IncludeOffset {
		next()
	}

	defer iter.Release()
	for next() {
		if err := iter.Error(); err != nil {
			break
		}

		if !iter.Valid() {
			break
		}

		_k, _v := iter.Key(), iter.Value()

		if _k == nil {
			break
		}

		newK := make([]byte, len(_k))
		newV := make([]byte, len(_v))

		copy(newK, _k)
		copy(newV, _v)

		decodedValue := BytesToValue(newV)
		if decodedValue.IsExpired() {
			continue
		}

		if !opts.Scanner(newK, decodedValue.Value) {
			break
		}
	}

	return nil
}
