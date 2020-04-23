package leveldb

import (
	"time"

	"github.com/alash3al/goukv"
	"github.com/vmihailenco/msgpack/v4"
)

// Value represents a value with expiration date
type Value struct {
	Value   []byte
	Expires *time.Time
}

// Bytes encodes the value to a byte array
func (e Value) Bytes() []byte {
	b, _ := msgpack.Marshal(e)
	return b
}

// IsExpired whether the value is expired or not
func (e Value) IsExpired() bool {
	if e.Expires == nil {
		return false
	}

	expires := *(e.Expires)
	return time.Now().After(expires) || time.Now().Equal(expires)
}

// EntryToValue build a value from entry representation
func EntryToValue(e *goukv.Entry) Value {
	val := Value{
		Value:   e.Value,
		Expires: nil,
	}

	if e.TTL > 0 {
		expires := time.Now().Add(e.TTL)
		val.Expires = &expires
	}

	return val
}

// BytesToValue Decodes the specified byte array to Value
func BytesToValue(b []byte) (v Value) {
	msgpack.Unmarshal(b, &v)
	return
}
