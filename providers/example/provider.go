package example

import (
	"time"

	"github.com/alash3al/goukv"
)

// Provider represents a driver
type Provider struct{}

// Open implements goukv.Open
func (p Provider) Open(opts map[string]interface{}) (goukv.Provider, error) {
	return nil, nil
}

// Put implements goukv.Put
func (p Provider) Put(e *goukv.Entry) error {
	return nil
}

// Batch perform multi put operation, empty value means *delete*
func (p Provider) Batch(entries []*goukv.Entry) error {
	return nil
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	return nil, nil
}

// TTL implements goukv.TTL
func (p Provider) TTL(k []byte) (*time.Time, error) {
	return nil, nil
}

// Delete implements goukv.Delete
func (p Provider) Delete(k []byte) error {
	return nil
}

// Close implements goukv.Close
func (p Provider) Close() error {
	return nil
}

// Scan implements goukv.Scan
func (p Provider) Scan(opts goukv.ScanOpts) {

}
